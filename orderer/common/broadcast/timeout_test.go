/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package broadcast_test

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	. "github.com/hyperledger/fabric/internal/test"
	"github.com/hyperledger/fabric/orderer/common/broadcast"
	"github.com/hyperledger/fabric/orderer/common/broadcast/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// blockingSupport is a hand-written ChannelSupport whose fake validator
// (ProcessNormalMsg/ProcessConfigUpdateMsg) and fake orderer
// (WaitReady/Order/Configure) can each be blocked on a gate channel. The gates
// are closed at cleanup time and release() waits until every in-flight fake
// method has returned, so no worker goroutine outlives a test.
type blockingSupport struct {
	*mock.ChannelSupport

	mu   sync.Mutex
	done map[chan struct{}]struct{}

	blockValidate chan struct{}
	blockWait     chan struct{}
	blockOrder    chan struct{}

	validateEntered chan struct{}
	waitEntered     chan struct{}
	orderEntered    chan struct{}

	validateOnce sync.Once
	waitOnce     sync.Once
	orderOnce    sync.Once

	// counts record calls made directly against blockingSupport; the
	// embedded counterfeiter mock counters stay zero because the overriding
	// methods below do not delegate to it.
	validateCount atomic.Int64
	waitCount     atomic.Int64
	orderCount    atomic.Int64

	validateReleased sync.Once
	waitReleased     sync.Once
	orderReleased    sync.Once
}

func newBlockingSupport() *blockingSupport {
	return &blockingSupport{
		ChannelSupport:  &mock.ChannelSupport{},
		done:            map[chan struct{}]struct{}{},
		blockValidate:   make(chan struct{}),
		blockWait:       make(chan struct{}),
		blockOrder:      make(chan struct{}),
		validateEntered: make(chan struct{}),
		waitEntered:     make(chan struct{}),
		orderEntered:    make(chan struct{}),
	}
}

// begin registers an in-flight gated operation and returns a signal channel
// that finish closes when the fake method returns. Pair it with finish via
// defer at the top of the fake method; release() then waits until every fake
// method body (including its deferred cleanup) has completed.
func (s *blockingSupport) begin() chan struct{} {
	ch := make(chan struct{})
	s.mu.Lock()
	s.done[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *blockingSupport) finish(ch chan struct{}) {
	s.mu.Lock()
	delete(s.done, ch)
	s.mu.Unlock()
	close(ch)
}

// release unblocks every gated call and waits until no fake method is
// executing, proving the cleanup is leak free.
func (s *blockingSupport) release() {
	s.validateReleased.Do(func() { close(s.blockValidate) })
	s.waitReleased.Do(func() { close(s.blockWait) })
	s.orderReleased.Do(func() { close(s.blockOrder) })
	for {
		s.mu.Lock()
		n := len(s.done)
		var wait <-chan struct{}
		if n > 0 {
			for ch := range s.done {
				wait = ch
				break
			}
		}
		s.mu.Unlock()
		if n == 0 {
			return
		}
		<-wait
	}
}

// releaseValidate lets the validation phase complete. Tests that exercise the
// ordering phase must call it so processing can leave validation.
func (s *blockingSupport) releaseValidate() {
	s.validateReleased.Do(func() { close(s.blockValidate) })
}

// releaseWait lets WaitReady return so processing proceeds to the enqueue call.
func (s *blockingSupport) releaseWait() {
	s.waitReleased.Do(func() { close(s.blockWait) })
}

func (s *blockingSupport) entered(ch *chan struct{}, once *sync.Once) {
	once.Do(func() { close(*ch) })
}

func (s *blockingSupport) ProcessNormalMsg(_ *cb.Envelope) (uint64, error) {
	s.validateCount.Add(1)
	done := s.begin()
	defer s.finish(done)
	s.entered(&s.validateEntered, &s.validateOnce)
	<-s.blockValidate
	return 5, nil
}

func (s *blockingSupport) ProcessConfigUpdateMsg(_ *cb.Envelope) (*cb.Envelope, uint64, error) {
	s.validateCount.Add(1)
	done := s.begin()
	defer s.finish(done)
	s.entered(&s.validateEntered, &s.validateOnce)
	<-s.blockValidate
	return &cb.Envelope{}, 5, nil
}

func (s *blockingSupport) WaitReady() error {
	s.waitCount.Add(1)
	done := s.begin()
	defer s.finish(done)
	s.entered(&s.waitEntered, &s.waitOnce)
	<-s.blockWait
	return nil
}

func (s *blockingSupport) Order(_ *cb.Envelope, _ uint64) error {
	s.orderCount.Add(1)
	done := s.begin()
	defer s.finish(done)
	s.entered(&s.orderEntered, &s.orderOnce)
	<-s.blockOrder
	return nil
}

func (s *blockingSupport) Configure(_ *cb.Envelope, _ uint64) error {
	s.orderCount.Add(1)
	done := s.begin()
	defer s.finish(done)
	s.entered(&s.orderEntered, &s.orderOnce)
	<-s.blockOrder
	return nil
}

var _ = Describe("Broadcast timeout classification", func() {
	var (
		registrar *mock.ChannelSupportRegistrar
		handler   *broadcast.Handler
		srv       *mock.ABServer
		support   *blockingSupport
		msg       *cb.Envelope
		ctx       context.Context
		cancel    context.CancelFunc
	)

	newMetrics := func() *broadcast.Metrics {
		validateHistogram := &mock.MetricsHistogram{}
		validateHistogram.WithReturns(validateHistogram)
		enqueueHistogram := &mock.MetricsHistogram{}
		enqueueHistogram.WithReturns(enqueueHistogram)
		processedCounter := &mock.MetricsCounter{}
		processedCounter.WithReturns(processedCounter)
		return &broadcast.Metrics{
			ValidateDuration: validateHistogram,
			EnqueueDuration:  enqueueHistogram,
			ProcessedCount:   processedCounter,
		}
	}

	BeforeEach(func() {
		msg = &cb.Envelope{}
		registrar = &mock.ChannelSupportRegistrar{}
		support = newBlockingSupport()
		registrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
			Type:      3,
			ChannelId: "fake-channel",
		}, false, support, nil)

		ctx, cancel = context.WithCancel(context.Background())
		srv = &mock.ABServer{}
		srv.ContextReturns(ctx)
		// First Recv hands the handler the message; the second Recv returns
		// EOF so a successful Handle terminates. Tests that cancel during
		// processing never reach the second Recv.
		srv.RecvReturnsOnCall(0, msg, nil)
		srv.RecvReturnsOnCall(1, nil, io.EOF)

		handler = &broadcast.Handler{
			SupportRegistrar: registrar,
			Metrics:          newMetrics(),
		}
	})

	AfterEach(func() {
		cancel()
		support.release()
	})

	waitEntered := func(ch <-chan struct{}) {
		Eventually(ch, 5*time.Second).Should(BeClosed())
	}

	Describe("distinct timeout categories via ProcessMessage", func() {
		It("classifies a blocked validator as BAD_REQUEST (validation timeout)", func() {
			handler.ValidateTimeout = 20 * time.Millisecond

			start := time.Now()
			resp := handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			elapsed := time.Since(start)

			Expect(resp).NotTo(BeNil())
			Expect(resp.GetStatus()).To(Equal(cb.Status_BAD_REQUEST))
			Expect(resp.Info).To(ContainSubstring("validation timed out"))
			Expect(elapsed).To(BeNumerically(">=", 15*time.Millisecond))
			Expect(elapsed).To(BeNumerically("<", time.Second))

			// The orderer must never have been touched by a validation timeout.
			Expect(support.waitCount.Load()).To(Equal(int64(0)))
			Expect(support.orderCount.Load()).To(Equal(int64(0)))
		})

		It("classifies a blocked WaitReady as SERVICE_UNAVAILABLE (ordering timeout)", func() {
			handler.OrderTimeout = 20 * time.Millisecond

			done := make(chan *ab.BroadcastResponse, 1)
			go func() {
				done <- handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			}()

			// Let validation finish; the WaitReady call then blocks.
			Eventually(support.validateEntered, 5*time.Second).Should(BeClosed())
			support.releaseValidate()
			waitEntered(support.waitEntered)

			start := time.Now()
			var resp *ab.BroadcastResponse
			Eventually(done, 5*time.Second).Should(Receive(&resp))
			elapsed := time.Since(start)

			Expect(resp).NotTo(BeNil())
			Expect(resp.GetStatus()).To(Equal(cb.Status_SERVICE_UNAVAILABLE))
			Expect(resp.Info).To(ContainSubstring("ordering timed out"))
			Expect(elapsed).To(BeNumerically("~", 20*time.Millisecond, 300*time.Millisecond))

			// Order() is only reached after WaitReady returns.
			Expect(support.orderCount.Load()).To(Equal(int64(0)))
		})

		It("classifies a blocked Order call itself as SERVICE_UNAVAILABLE", func() {
			handler.OrderTimeout = 100 * time.Millisecond

			// WaitReady must not block; only the enqueue call does.
			support.releaseWait()

			done := make(chan *ab.BroadcastResponse, 1)
			go func() {
				done <- handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			}()

			Eventually(support.validateEntered, 5*time.Second).Should(BeClosed())
			support.releaseValidate()
			waitEntered(support.orderEntered)

			var resp *ab.BroadcastResponse
			Eventually(done, 5*time.Second).Should(Receive(&resp))
			Expect(resp.GetStatus()).To(Equal(cb.Status_SERVICE_UNAVAILABLE))
			Expect(resp.Info).To(ContainSubstring("ordering timed out"))
			Expect(support.orderCount.Load()).To(Equal(int64(1)))
		})

		It("uses the same validation category for config messages", func() {
			registrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
				Type:      1,
				ChannelId: "fake-channel",
			}, true, support, nil)
			handler.ValidateTimeout = 20 * time.Millisecond

			resp := handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			Expect(resp).NotTo(BeNil())
			Expect(resp.GetStatus()).To(Equal(cb.Status_BAD_REQUEST))
			Expect(resp.Info).To(ContainSubstring("validation timed out"))
			Expect(support.orderCount.Load()).To(Equal(int64(0)))
		})
	})

	Describe("client cancellation", func() {
		It("stops waiting during validation and answers nothing", func() {
			done := make(chan *ab.BroadcastResponse, 1)
			go func() {
				done <- handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			}()
			waitEntered(support.validateEntered)

			cancel()
			var resp *ab.BroadcastResponse
			Eventually(done, 5*time.Second).Should(Receive(&resp))
			Expect(resp).To(BeNil())
		})

		It("stops waiting during ordering and answers nothing", func() {
			done := make(chan *ab.BroadcastResponse, 1)
			go func() {
				done <- handler.ProcessMessage(ctx, msg, "127.0.0.1:0")
			}()
			waitEntered(support.validateEntered)
			support.releaseValidate()
			waitEntered(support.waitEntered)

			cancel()
			var resp *ab.BroadcastResponse
			Eventually(done, 5*time.Second).Should(Receive(&resp))
			Expect(resp).To(BeNil())
		})

		It("never sends a BroadcastResponse through Handle after cancellation", func() {
			handleDone := make(chan error, 1)
			go func() {
				handleDone <- handler.Handle(srv)
			}()
			waitEntered(support.validateEntered)

			cancel()
			Eventually(handleDone, 5*time.Second).Should(Receive(BeNil()))
			Expect(srv.SendCallCount()).To(Equal(0))
		})

		It("answers nothing when canceled during the ordering phase", func() {
			handleDone := make(chan error, 1)
			go func() {
				handleDone <- handler.Handle(srv)
			}()
			waitEntered(support.validateEntered)
			support.releaseValidate()
			waitEntered(support.waitEntered)

			cancel()
			Eventually(handleDone, 5*time.Second).Should(Receive(BeNil()))
			Expect(srv.SendCallCount()).To(Equal(0))
		})
	})

	Describe("end-to-end classification through Handle", func() {
		It("replies once with BAD_REQUEST when validation times out", func() {
			handler.ValidateTimeout = 20 * time.Millisecond

			err := handler.Handle(srv)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.SendCallCount()).To(Equal(1))
			resp := srv.SendArgsForCall(0)
			Expect(resp.GetStatus()).To(Equal(cb.Status_BAD_REQUEST))
			Expect(resp.Info).To(ContainSubstring("validation timed out"))
		})

		It("replies once with SERVICE_UNAVAILABLE when ordering times out", func() {
			handler.OrderTimeout = 20 * time.Millisecond

			handleDone := make(chan error, 1)
			go func() {
				handleDone <- handler.Handle(srv)
			}()
			waitEntered(support.validateEntered)
			support.releaseValidate()
			waitEntered(support.waitEntered)

			Eventually(handleDone, 5*time.Second).Should(Receive(BeNil()))
			Expect(srv.SendCallCount()).To(Equal(1))
			resp := srv.SendArgsForCall(0)
			Expect(resp.GetStatus()).To(Equal(cb.Status_SERVICE_UNAVAILABLE))
			Expect(resp.Info).To(ContainSubstring("ordering timed out"))
		})
	})

	Describe("success path is unchanged", func() {
		It("enqueues and replies SUCCESS without configuring timeouts", func() {
			support.release()

			err := handler.Handle(srv)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.SendCallCount()).To(Equal(1))
			Expect(srv.SendArgsForCall(0)).To(ProtoEqual(
				&ab.BroadcastResponse{Status: cb.Status_SUCCESS},
			))
			Expect(support.validateCount.Load()).To(Equal(int64(1)))
			Expect(support.waitCount.Load()).To(Equal(int64(1)))
			Expect(support.orderCount.Load()).To(Equal(int64(1)))
		})
	})
})
