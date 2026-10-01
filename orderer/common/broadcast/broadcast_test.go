/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package broadcast_test

import (
	"context"
	"errors"
	"io"
	"time"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	. "github.com/hyperledger/fabric/internal/test"
	"github.com/hyperledger/fabric/orderer/common/broadcast"
	"github.com/hyperledger/fabric/orderer/common/broadcast/mock"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Broadcast", func() {
	var (
		fakeSupportRegistrar  *mock.ChannelSupportRegistrar
		handler               *broadcast.Handler
		fakeValidateHistogram *mock.MetricsHistogram
		fakeEnqueueHistogram  *mock.MetricsHistogram
		fakeProcessedCounter  *mock.MetricsCounter
	)

	BeforeEach(func() {
		fakeSupportRegistrar = &mock.ChannelSupportRegistrar{}

		fakeValidateHistogram = &mock.MetricsHistogram{}
		fakeValidateHistogram.WithReturns(fakeValidateHistogram)

		fakeEnqueueHistogram = &mock.MetricsHistogram{}
		fakeEnqueueHistogram.WithReturns(fakeEnqueueHistogram)

		fakeProcessedCounter = &mock.MetricsCounter{}
		fakeProcessedCounter.WithReturns(fakeProcessedCounter)

		handler = &broadcast.Handler{
			SupportRegistrar: fakeSupportRegistrar,
			Metrics: &broadcast.Metrics{
				ValidateDuration: fakeValidateHistogram,
				EnqueueDuration:  fakeEnqueueHistogram,
				ProcessedCount:   fakeProcessedCounter,
			},
		}
	})

	Describe("Handle", func() {
		var (
			fakeABServer *mock.ABServer
			fakeSupport  *mock.ChannelSupport
			fakeMsg      *cb.Envelope
		)

		BeforeEach(func() {
			fakeMsg = &cb.Envelope{}

			fakeABServer = &mock.ABServer{}
			fakeABServer.ContextReturns(context.TODO())
			fakeABServer.RecvReturns(fakeMsg, nil)
			fakeABServer.RecvReturnsOnCall(1, nil, io.EOF)

			fakeSupport = &mock.ChannelSupport{}
			fakeSupport.ProcessNormalMsgReturns(5, nil)

			fakeSupportRegistrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
				Type:      3,
				ChannelId: "fake-channel",
			}, false, fakeSupport, nil)
		})

		It("enqueues the message to the consenter", func() {
			err := handler.Handle(fakeABServer)
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeABServer.RecvCallCount()).To(Equal(2))
			Expect(fakeSupportRegistrar.BroadcastChannelSupportCallCount()).To(Equal(1))
			Expect(fakeSupportRegistrar.BroadcastChannelSupportArgsForCall(0)).To(Equal(fakeMsg))

			Expect(fakeSupport.ProcessNormalMsgCallCount()).To(Equal(1))
			Expect(fakeSupport.ProcessNormalMsgArgsForCall(0)).To(Equal(fakeMsg))

			Expect(fakeSupport.WaitReadyCallCount()).To(Equal(1))

			Expect(fakeSupport.OrderCallCount()).To(Equal(1))
			orderedMsg, seq := fakeSupport.OrderArgsForCall(0)
			Expect(orderedMsg).To(Equal(fakeMsg))
			Expect(seq).To(Equal(uint64(5)))

			Expect(fakeValidateHistogram.WithCallCount()).To(Equal(1))
			Expect(fakeValidateHistogram.WithArgsForCall(0)).To(Equal([]string{
				"status", "SUCCESS",
				"channel", "fake-channel",
				"type", "ENDORSER_TRANSACTION",
			}))
			Expect(fakeValidateHistogram.ObserveCallCount()).To(Equal(1))
			Expect(fakeValidateHistogram.ObserveArgsForCall(0)).To(BeNumerically(">=", 0))
			Expect(fakeValidateHistogram.ObserveArgsForCall(0)).To(BeNumerically("<", 1))

			Expect(fakeEnqueueHistogram.WithCallCount()).To(Equal(1))
			Expect(fakeEnqueueHistogram.WithArgsForCall(0)).To(Equal([]string{
				"status", "SUCCESS",
				"channel", "fake-channel",
				"type", "ENDORSER_TRANSACTION",
			}))
			Expect(fakeEnqueueHistogram.ObserveCallCount()).To(Equal(1))
			Expect(fakeEnqueueHistogram.ObserveArgsForCall(0)).To(BeNumerically(">=", 0))
			Expect(fakeEnqueueHistogram.ObserveArgsForCall(0)).To(BeNumerically("<", 1))

			Expect(fakeProcessedCounter.WithCallCount()).To(Equal(1))
			Expect(fakeProcessedCounter.WithArgsForCall(0)).To(Equal([]string{
				"status", "SUCCESS",
				"channel", "fake-channel",
				"type", "ENDORSER_TRANSACTION",
			}))
			Expect(fakeProcessedCounter.AddCallCount()).To(Equal(1))
			Expect(fakeProcessedCounter.AddArgsForCall(0)).To(Equal(float64(1)))

			Expect(fakeABServer.SendCallCount()).To(Equal(1))
			Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
				&ab.BroadcastResponse{Status: cb.Status_SUCCESS},
			))
		})

		Context("when the channel support cannot be retrieved", func() {
			BeforeEach(func() {
				fakeSupportRegistrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
					Type:      2,
					ChannelId: "fake-channel",
				}, false, nil, errors.New("support-error"))
			})

			It("returns the error to the client with a bad status", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).NotTo(HaveOccurred())

				Expect(fakeABServer.SendCallCount()).To(Equal(1))
				Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
					&ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: "support-error"},
				))
			})

			Context("when the channel header is not validly decoded", func() {
				BeforeEach(func() {
					fakeSupportRegistrar.BroadcastChannelSupportReturns(nil, false, nil, errors.New("support-error"))
				})

				It("does not crash", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeValidateHistogram.WithCallCount()).To(Equal(1))
					Expect(fakeValidateHistogram.WithArgsForCall(0)).To(Equal([]string{
						"status", "BAD_REQUEST",
						"channel", "unknown",
						"type", "unknown",
					}))
					Expect(fakeEnqueueHistogram.WithCallCount()).To(Equal(0))
					Expect(fakeProcessedCounter.WithCallCount()).To(Equal(1))
				})
			})
		})

		Context("when the receive from the client fails", func() {
			BeforeEach(func() {
				fakeABServer.RecvReturns(nil, errors.New("recv-error"))
			})

			It("returns the error", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).To(MatchError("recv-error"))
			})
		})

		Context("when the consenter is not ready for the request", func() {
			BeforeEach(func() {
				fakeSupport.WaitReadyReturns(errors.New("not-ready"))
			})

			It("returns the error to the client with a service unavailable status", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).NotTo(HaveOccurred())

				Expect(fakeABServer.SendCallCount()).To(Equal(1))
				Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
					&ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: "not-ready"},
				))
			})
		})

		Context("when the send to the client fails", func() {
			BeforeEach(func() {
				fakeABServer.SendReturns(errors.New("send-error"))
			})

			It("returns the error", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).To(MatchError("send-error"))
			})
		})

		Context("when the consenter cannot enqueue the message", func() {
			BeforeEach(func() {
				fakeSupport.OrderReturns(errors.New("consenter-error"))
			})

			It("returns the error", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).NotTo(HaveOccurred())

				Expect(fakeABServer.SendCallCount()).To(Equal(1))
				Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
					&ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: "consenter-error"},
				))
			})
		})

		Context("when the message processor returns an error", func() {
			BeforeEach(func() {
				fakeSupport.ProcessNormalMsgReturns(0, errors.New("normal-message-processing-error"))
			})

			It("returns the error and an error status", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).NotTo(HaveOccurred())

				Expect(fakeABServer.SendCallCount()).To(Equal(1))
				Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
					&ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: "normal-message-processing-error"},
				))
			})

			Context("when the error cause is msgprocessor.ErrChannelDoesNotExist", func() {
				BeforeEach(func() {
					fakeSupport.ProcessNormalMsgReturns(0, msgprocessor.ErrChannelDoesNotExist)
				})

				It("returns the error and a not found status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: cb.Status_NOT_FOUND, Info: msgprocessor.ErrChannelDoesNotExist.Error()},
					))
				})
			})

			Context("when the error cause is msgprocessor.ErrPermissionDenied", func() {
				BeforeEach(func() {
					fakeSupport.ProcessNormalMsgReturns(0, msgprocessor.ErrPermissionDenied)
				})

				It("returns the error and a not found status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: cb.Status_FORBIDDEN, Info: msgprocessor.ErrPermissionDenied.Error()},
					))
				})
			})
		})

		Context("when the message is a config message", func() {
			var fakeConfig *cb.Envelope

			BeforeEach(func() {
				fakeConfig = &cb.Envelope{}

				fakeSupportRegistrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
					Type:      1,
					ChannelId: "fake-channel",
				}, true, fakeSupport, nil)

				fakeSupport.ProcessConfigUpdateMsgReturns(fakeConfig, 3, nil)
			})

			It("enqueues the message as a config message to the consenter", func() {
				err := handler.Handle(fakeABServer)
				Expect(err).NotTo(HaveOccurred())

				Expect(fakeSupport.ProcessNormalMsgCallCount()).To(Equal(0))
				Expect(fakeSupport.ProcessConfigUpdateMsgCallCount()).To(Equal(1))
				Expect(fakeSupport.ProcessConfigUpdateMsgArgsForCall(0)).To(Equal(fakeMsg))

				Expect(fakeSupport.WaitReadyCallCount()).To(Equal(1))

				Expect(fakeSupport.OrderCallCount()).To(Equal(0))
				Expect(fakeSupport.ConfigureCallCount()).To(Equal(1))
				configMsg, seq := fakeSupport.ConfigureArgsForCall(0)
				Expect(configMsg).To(Equal(fakeConfig))
				Expect(seq).To(Equal(uint64(3)))

				Expect(fakeABServer.SendCallCount()).To(Equal(1))
				Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(&ab.BroadcastResponse{Status: cb.Status_SUCCESS}))
			})

			Context("when the consenter is not ready for the request", func() {
				BeforeEach(func() {
					fakeSupport.WaitReadyReturns(errors.New("not-ready"))
				})

				It("returns the error to the client with a service unavailable status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: "not-ready"},
					))
				})
			})

			Context("when the consenter cannot enqueue the message", func() {
				BeforeEach(func() {
					fakeSupport.ConfigureReturns(errors.New("consenter-error"))
				})

				It("returns the error", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: "consenter-error"},
					))
				})
			})

			Context("when the processing of the config update fails", func() {
				BeforeEach(func() {
					fakeSupport.ProcessConfigUpdateMsgReturns(nil, 0, errors.New("config-processing-error"))
				})

				It("returns the error with a bad_status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: "config-processing-error"},
					))
				})

				Context("when the error cause is msgprocessor.ErrChannelDoesNotExist", func() {
					BeforeEach(func() {
						fakeSupport.ProcessConfigUpdateMsgReturns(nil, 0, msgprocessor.ErrChannelDoesNotExist)
					})

					It("returns the error and a not found status", func() {
						err := handler.Handle(fakeABServer)
						Expect(err).NotTo(HaveOccurred())

						Expect(fakeABServer.SendCallCount()).To(Equal(1))
						Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
							&ab.BroadcastResponse{Status: cb.Status_NOT_FOUND, Info: msgprocessor.ErrChannelDoesNotExist.Error()},
						))
					})
				})

				Context("when the error cause is msgprocessor.ErrPermissionDenied", func() {
					BeforeEach(func() {
						fakeSupport.ProcessConfigUpdateMsgReturns(nil, 0, msgprocessor.ErrPermissionDenied)
					})

					It("returns the error and a not found status", func() {
						err := handler.Handle(fakeABServer)
						Expect(err).NotTo(HaveOccurred())

						Expect(fakeABServer.SendCallCount()).To(Equal(1))
						Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
							&ab.BroadcastResponse{Status: cb.Status_FORBIDDEN, Info: msgprocessor.ErrPermissionDenied.Error()},
						))
					})
				})
			})
		})

		Context("timeout classification", func() {
			var blockCh chan struct{}

			BeforeEach(func() {
				blockCh = make(chan struct{})
			})

			AfterEach(func() {
				// Unblock any goroutine still waiting on the blocking call so
				// the fake does not leak past the test.
				close(blockCh)
			})

			Context("when validation of a normal message times out", func() {
				BeforeEach(func() {
					fakeSupport.ProcessNormalMsgCalls(func(msg *cb.Envelope) (uint64, error) {
						<-blockCh
						return 0, nil
					})
					handler.ValidateTimeout = 50 * time.Millisecond
				})

				It("returns a validation timeout status and closes the stream", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: broadcast.StatusValidationTimeout, Info: "validation timeout"},
					))

					Expect(fakeProcessedCounter.WithCallCount()).To(Equal(1))
					Expect(fakeProcessedCounter.WithArgsForCall(0)).To(Equal([]string{
						"status", "VALIDATION_TIMEOUT",
						"channel", "fake-channel",
						"type", "ENDORSER_TRANSACTION",
					}))
				})
			})

			Context("when ordering of a normal message times out", func() {
				BeforeEach(func() {
					fakeSupport.OrderCalls(func(msg *cb.Envelope, configSeq uint64) error {
						<-blockCh
						return nil
					})
					handler.OrderTimeout = 50 * time.Millisecond
				})

				It("returns an ordering timeout status and closes the stream", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: broadcast.StatusOrderingTimeout, Info: "ordering timeout"},
					))

					Expect(fakeProcessedCounter.WithCallCount()).To(Equal(1))
					Expect(fakeProcessedCounter.WithArgsForCall(0)).To(Equal([]string{
						"status", "ORDERING_TIMEOUT",
						"channel", "fake-channel",
						"type", "ENDORSER_TRANSACTION",
					}))
				})
			})

			Context("when validation of a config message times out", func() {
				BeforeEach(func() {
					fakeSupportRegistrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
						Type:      1,
						ChannelId: "fake-channel",
					}, true, fakeSupport, nil)
					fakeSupport.ProcessConfigUpdateMsgCalls(func(msg *cb.Envelope) (*cb.Envelope, uint64, error) {
						<-blockCh
						return nil, 0, nil
					})
					handler.ValidateTimeout = 50 * time.Millisecond
				})

				It("returns a validation timeout status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: broadcast.StatusValidationTimeout, Info: "validation timeout"},
					))
				})
			})

			Context("when ordering of a config message times out", func() {
				BeforeEach(func() {
					fakeSupportRegistrar.BroadcastChannelSupportReturns(&cb.ChannelHeader{
						Type:      1,
						ChannelId: "fake-channel",
					}, true, fakeSupport, nil)
					fakeSupport.ProcessConfigUpdateMsgReturns(&cb.Envelope{}, 3, nil)
					fakeSupport.ConfigureCalls(func(config *cb.Envelope, configSeq uint64) error {
						<-blockCh
						return nil
					})
					handler.OrderTimeout = 50 * time.Millisecond
				})

				It("returns an ordering timeout status", func() {
					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(1))
					Expect(fakeABServer.SendArgsForCall(0)).To(ProtoEqual(
						&ab.BroadcastResponse{Status: broadcast.StatusOrderingTimeout, Info: "ordering timeout"},
					))
				})
			})
		})

		Context("client cancellation", func() {
			var (
				blockCh chan struct{}
				cancel  context.CancelFunc
			)

			BeforeEach(func() {
				blockCh = make(chan struct{})
				var ctx context.Context
				ctx, cancel = context.WithCancel(context.Background())
				fakeABServer.ContextReturns(ctx)
			})

			AfterEach(func() {
				// Unblock any goroutine still waiting on the blocking call.
				close(blockCh)
			})

			Context("when the client cancels during validation", func() {
				BeforeEach(func() {
					fakeSupport.ProcessNormalMsgCalls(func(msg *cb.Envelope) (uint64, error) {
						<-blockCh
						return 0, nil
					})
				})

				It("stops waiting and does not send a response", func() {
					go func() {
						time.Sleep(20 * time.Millisecond)
						cancel()
					}()

					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(0))
					Expect(fakeProcessedCounter.WithCallCount()).To(Equal(0))
				})
			})

			Context("when the client cancels during ordering", func() {
				BeforeEach(func() {
					fakeSupport.OrderCalls(func(msg *cb.Envelope, configSeq uint64) error {
						<-blockCh
						return nil
					})
				})

				It("stops waiting and does not send a response", func() {
					go func() {
						time.Sleep(20 * time.Millisecond)
						cancel()
					}()

					err := handler.Handle(fakeABServer)
					Expect(err).NotTo(HaveOccurred())

					Expect(fakeABServer.SendCallCount()).To(Equal(0))
					Expect(fakeProcessedCounter.WithCallCount()).To(Equal(0))
				})
			})
		})
	})
})
