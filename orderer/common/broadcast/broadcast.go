/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package broadcast

import (
	"context"
	errors2 "errors"
	"io"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("orderer.common.broadcast")

// ErrValidationTimeout is returned (wrapped) when the message validation phase
// (ProcessNormalMsg/ProcessConfigUpdateMsg) does not complete within the
// configured ValidateTimeout. It is reported to the client as BAD_REQUEST.
var ErrValidationTimeout = errors.New("broadcast validation timed out")

// ErrOrderTimeout is returned (wrapped) when the ordering phase
// (WaitReady/Order/Configure) does not complete within the configured
// OrderTimeout. It is reported to the client as SERVICE_UNAVAILABLE.
var ErrOrderTimeout = errors.New("broadcast ordering timed out")

//go:generate counterfeiter -o mock/channel_support_registrar.go --fake-name ChannelSupportRegistrar . ChannelSupportRegistrar

// ChannelSupportRegistrar provides a way for the Handler to look up the Support for a channel
type ChannelSupportRegistrar interface {
	// BroadcastChannelSupport returns the message channel header, whether the message is a config update
	// and the channel resources for a message or an error if the message is not a message which can
	// be processed directly (like CONFIG and ORDERER_TRANSACTION messages)
	BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, ChannelSupport, error)
}

//go:generate counterfeiter -o mock/channel_support.go --fake-name ChannelSupport . ChannelSupport

// ChannelSupport provides the backing resources needed to support broadcast on a channel
type ChannelSupport interface {
	msgprocessor.Processor
	Consenter
}

// Consenter provides methods to send messages through consensus
type Consenter interface {
	// Order accepts a message or returns an error indicating the cause of failure
	// It ultimately passes through to the consensus.Chain interface
	Order(env *cb.Envelope, configSeq uint64) error

	// Configure accepts a reconfiguration or returns an error indicating the cause of failure
	// It ultimately passes through to the consensus.Chain interface
	Configure(config *cb.Envelope, configSeq uint64) error

	// WaitReady blocks waiting for consenter to be ready for accepting new messages.
	// This is useful when consenter needs to temporarily block ingress messages so
	// that in-flight messages can be consumed. It could return error if consenter is
	// in erroneous states. If this blocking behavior is not desired, consenter could
	// simply return nil.
	WaitReady() error
}

// Handler is designed to handle connections from Broadcast AB gRPC service
type Handler struct {
	SupportRegistrar ChannelSupportRegistrar
	Metrics          *Metrics
	// ValidateTimeout bounds how long a single message may spend in the
	// validation phase (ProcessNormalMsg/ProcessConfigUpdateMsg). A value of
	// zero means the phase is unbounded.
	ValidateTimeout time.Duration
	// OrderTimeout bounds how long a single message may spend in the
	// ordering phase, which spans WaitReady and Order/Configure. A value of
	// zero means the phase is unbounded.
	OrderTimeout time.Duration
}

// Handle reads requests from a Broadcast stream, processes them, and returns the responses to the stream
func (bh *Handler) Handle(srv ab.AtomicBroadcast_BroadcastServer) error {
	ctx := srv.Context()
	addr := util.ExtractRemoteAddress(ctx)
	logger.Debugf("Starting new broadcast loop for %s", addr)
	for {
		msg, err := srv.Recv()
		if errors2.Is(err, io.EOF) {
			logger.Debugf("Received EOF from %s, hangup", addr)
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				logger.Debugf("Client %s gone while receiving: %s", addr, ctx.Err())
				return nil
			}
			logger.Warningf("Error reading from %s: %s", addr, err)
			return err
		}

		resp := bh.ProcessMessage(ctx, msg, addr)
		// ProcessMessage returns nil when the client goes away while the
		// message is being processed: do not attempt a reply on a dead
		// stream and make sure only one response is ever produced.
		if resp == nil {
			logger.Debugf("Client %s canceled, closing broadcast stream without a response", addr)
			return nil
		}
		if err = srv.Send(resp); err != nil {
			if ctx.Err() != nil {
				logger.Debugf("Client %s gone while sending: %s", addr, ctx.Err())
				return nil
			}
			logger.Warningf("Error sending to %s: %s", addr, err)
			return err
		}
		if resp.GetStatus() != cb.Status_SUCCESS {
			return nil
		}
	}
}

type MetricsTracker struct {
	ValidateStartTime time.Time
	EnqueueStartTime  time.Time
	ValidateDuration  time.Duration
	ChannelID         string
	TxType            string
	Metrics           *Metrics
}

func (mt *MetricsTracker) Record(resp *ab.BroadcastResponse) {
	labels := []string{
		"status", resp.GetStatus().String(),
		"channel", mt.ChannelID,
		"type", mt.TxType,
	}

	if mt.ValidateDuration == 0 {
		mt.EndValidate()
	}
	mt.Metrics.ValidateDuration.With(labels...).Observe(mt.ValidateDuration.Seconds())

	if mt.EnqueueStartTime != (time.Time{}) {
		enqueueDuration := time.Since(mt.EnqueueStartTime)
		mt.Metrics.EnqueueDuration.With(labels...).Observe(enqueueDuration.Seconds())
	}

	mt.Metrics.ProcessedCount.With(labels...).Add(1)
}

func (mt *MetricsTracker) BeginValidate() {
	mt.ValidateStartTime = time.Now()
}

func (mt *MetricsTracker) EndValidate() {
	mt.ValidateDuration = time.Since(mt.ValidateStartTime)
}

func (mt *MetricsTracker) BeginEnqueue() {
	mt.EnqueueStartTime = time.Now()
}

type callResult[T any] struct {
	value T
	err   error
}

type configUpdateResult struct {
	config    *cb.Envelope
	configSeq uint64
}

// runWithTimeout executes fn in a goroutine and waits for it to finish, for
// timeout to elapse, or for ctx to be canceled. A timeout value of zero means
// the wait is unbounded. On timeout the returned error wraps timeoutErr; when
// ctx is canceled the context error is returned instead. The buffered result
// channel ensures the worker goroutine never blocks, even if the caller has
// already stopped waiting.
func runWithTimeout[T any](ctx context.Context, timeout time.Duration, timeoutErr error, fn func() (T, error)) (T, error) {
	var zero T
	ch := make(chan callResult[T], 1)
	go func() {
		value, err := fn()
		ch <- callResult[T]{value: value, err: err}
	}()

	var timeoutC <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutC = timer.C
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timeoutC:
		return zero, errors.Wrapf(timeoutErr, "after %s", timeout)
	case r := <-ch:
		return r.value, r.err
	}
}

// ProcessMessage validates and enqueues a single message. It returns nil when
// the client cancels the request while processing is still in flight; in that
// case no response must be sent on the stream.
func (bh *Handler) ProcessMessage(ctx context.Context, msg *cb.Envelope, addr string) (resp *ab.BroadcastResponse) {
	tracker := &MetricsTracker{
		ChannelID: "unknown",
		TxType:    "unknown",
		Metrics:   bh.Metrics,
	}
	defer func() {
		// This looks a little unnecessary, but if done directly as
		// a defer, resp gets the (always nil) current state of resp
		// and not the return value.
		// A nil resp means the client went away; nothing can be recorded
		// against a response and no reply is sent.
		if resp != nil {
			tracker.Record(resp)
		}
	}()
	tracker.BeginValidate()

	chdr, isConfig, processor, err := bh.SupportRegistrar.BroadcastChannelSupport(msg)
	if chdr != nil {
		tracker.ChannelID = chdr.GetChannelId()
		tracker.TxType = cb.HeaderType(chdr.GetType()).String()
	}
	if err != nil {
		logger.Warningf("[channel: %s] Could not get message processor for serving %s: %s", tracker.ChannelID, addr, err)
		return &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}
	}

	if !isConfig {
		logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s' of type %s", chdr.GetChannelId(), addr, chdr.GetTxId(), cb.HeaderType_name[chdr.GetType()])

		configSeq, err := runWithTimeout(ctx, bh.ValidateTimeout, ErrValidationTimeout, func() (uint64, error) {
			return processor.ProcessNormalMsg(msg)
		})
		if err != nil {
			if errors2.Is(err, ErrValidationTimeout) {
				logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with BAD_REQUEST: validation timed out: %s", chdr.GetChannelId(), addr, err)
				return &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}
			}
			if ctx.Err() != nil {
				logger.Debugf("[channel: %s] Client %s canceled during validation: %s", chdr.GetChannelId(), addr, err)
				return nil
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		err = bh.order(ctx, func() error { return processor.WaitReady() }, func() error {
			return processor.Order(msg, configSeq)
		})
		if err != nil {
			return orderErrorResponse(chdr.GetChannelId(), addr, "normal", ctx, err)
		}
	} else { // isConfig
		logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.GetChannelId(), addr)

		result, err := runWithTimeout(ctx, bh.ValidateTimeout, ErrValidationTimeout, func() (configUpdateResult, error) {
			config, configSeq, err := processor.ProcessConfigUpdateMsg(msg)
			return configUpdateResult{config: config, configSeq: configSeq}, err
		})
		if err != nil {
			if errors2.Is(err, ErrValidationTimeout) {
				logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with BAD_REQUEST: validation timed out: %s", chdr.GetChannelId(), addr, err)
				return &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}
			}
			if ctx.Err() != nil {
				logger.Debugf("[channel: %s] Client %s canceled during validation: %s", chdr.GetChannelId(), addr, err)
				return nil
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		err = bh.order(ctx, func() error { return processor.WaitReady() }, func() error {
			return processor.Configure(result.config, result.configSeq)
		})
		if err != nil {
			return orderErrorResponse(chdr.GetChannelId(), addr, "config", ctx, err)
		}
	}

	logger.Debugf("[channel: %s] Broadcast has successfully enqueued message of type %s from %s", chdr.GetChannelId(), cb.HeaderType_name[chdr.GetType()], addr)

	return &ab.BroadcastResponse{Status: cb.Status_SUCCESS}
}

// orderErrorResponse maps an ordering-phase error to its broadcast response,
// or returns nil when the client has canceled.
func orderErrorResponse(channelID, addr, msgKind string, ctx context.Context, err error) *ab.BroadcastResponse {
	if errors2.Is(err, ErrOrderTimeout) {
		logger.Warningf("[channel: %s] Rejecting broadcast of %s message from %s with SERVICE_UNAVAILABLE: ordering timed out: %s", channelID, msgKind, addr, err)
		return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
	}
	if ctx.Err() != nil {
		logger.Debugf("[channel: %s] Client %s canceled during ordering: %s", channelID, addr, err)
		return nil
	}
	logger.Warningf("[channel: %s] Rejecting broadcast of %s message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", channelID, msgKind, addr, err)
	return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
}

// order runs waitReady followed by enqueue under a single OrderTimeout budget
// so that time spent in WaitReady leaves less budget for Order/Configure. A
// non-positive OrderTimeout means the ordering phase is unbounded. Client
// cancellation (via ctx) is always observed promptly.
func (bh *Handler) order(ctx context.Context, waitReady, enqueue func() error) error {
	if bh.OrderTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bh.OrderTimeout)
		defer cancel()
	}

	if _, err := runWithTimeout(ctx, 0, ErrOrderTimeout, func() (struct{}, error) {
		return struct{}{}, waitReady()
	}); err != nil {
		if bh.OrderTimeout > 0 && errors2.Is(err, context.DeadlineExceeded) {
			return errors.Wrapf(ErrOrderTimeout, "after %s", bh.OrderTimeout)
		}
		return err
	}

	if _, err := runWithTimeout(ctx, 0, ErrOrderTimeout, func() (struct{}, error) {
		return struct{}{}, enqueue()
	}); err != nil {
		if bh.OrderTimeout > 0 && errors2.Is(err, context.DeadlineExceeded) {
			return errors.Wrapf(ErrOrderTimeout, "after %s", bh.OrderTimeout)
		}
		return err
	}
	return nil
}

// ClassifyError converts an error type into a status code.
func ClassifyError(err error) cb.Status {
	switch err := errors.Cause(err); {
	case errors2.Is(err, msgprocessor.ErrChannelDoesNotExist):
		return cb.Status_NOT_FOUND
	case errors2.Is(err, msgprocessor.ErrPermissionDenied):
		return cb.Status_FORBIDDEN
	case errors2.Is(err, msgprocessor.ErrMaintenanceMode):
		return cb.Status_SERVICE_UNAVAILABLE
	default:
		return cb.Status_BAD_REQUEST
	}
}
