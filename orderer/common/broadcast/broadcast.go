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

// Default timeouts applied to the validation and ordering phases when the
// Handler is constructed without explicit values.
const (
	DefaultValidateTimeout = 30 * time.Second
	DefaultOrderTimeout    = 30 * time.Second
)

// Distinguishable status codes returned when the validation or ordering phase
// of broadcast processing does not complete within its allotted timeout.
const (
	// StatusValidationTimeout indicates that the message validator did not
	// complete within the configured validation timeout.
	StatusValidationTimeout = cb.Status(504)
	// StatusOrderingTimeout indicates that the consenter did not complete the
	// order (or configure) operation within the configured ordering timeout.
	StatusOrderingTimeout = cb.Status(505)
)

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
	// ValidateTimeout bounds the time spent validating a single message.
	// A zero value applies DefaultValidateTimeout.
	ValidateTimeout time.Duration
	// OrderTimeout bounds the time spent enqueuing a single message to the
	// consenter. A zero value applies DefaultOrderTimeout.
	OrderTimeout time.Duration
}

func (bh *Handler) validateTimeout() time.Duration {
	if bh.ValidateTimeout > 0 {
		return bh.ValidateTimeout
	}
	return DefaultValidateTimeout
}

func (bh *Handler) orderTimeout() time.Duration {
	if bh.OrderTimeout > 0 {
		return bh.OrderTimeout
	}
	return DefaultOrderTimeout
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
			logger.Warningf("Error reading from %s: %s", addr, err)
			return err
		}

		resp, ok := bh.ProcessMessage(ctx, msg, addr)
		if !ok {
			// The client context was canceled while the message was being
			// processed; the stream is no longer usable, so stop waiting and
			// do not send a response.
			logger.Debugf("Context canceled while processing message from %s, stopping broadcast loop", addr)
			return nil
		}
		err = srv.Send(resp)
		if resp.GetStatus() != cb.Status_SUCCESS {
			return err
		}

		if err != nil {
			logger.Warningf("Error sending to %s: %s", addr, err)
			return err
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
		"status", StatusName(resp.GetStatus()),
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

// StatusName returns a stable label value for a status code. The proto
// generated enum only knows the built-in statuses, so the timeout categories
// are mapped to descriptive names here.
func StatusName(status cb.Status) string {
	switch status {
	case StatusValidationTimeout:
		return "VALIDATION_TIMEOUT"
	case StatusOrderingTimeout:
		return "ORDERING_TIMEOUT"
	default:
		return status.String()
	}
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

// ProcessMessage validates and enqueues a single message. The returned ok
// flag is false when the client context was canceled while the message was
// being processed; in that case no response should be sent.
func (bh *Handler) ProcessMessage(ctx context.Context, msg *cb.Envelope, addr string) (resp *ab.BroadcastResponse, ok bool) {
	tracker := &MetricsTracker{
		ChannelID: "unknown",
		TxType:    "unknown",
		Metrics:   bh.Metrics,
	}
	defer func() {
		// This looks a little unnecessary, but if done directly as
		// a defer, resp gets the (always nil) current state of resp
		// and not the return value. When the client context is canceled
		// resp is nil and no response is sent, so there is nothing to record.
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
		return &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}, true
	}

	if !isConfig {
		logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s' of type %s", chdr.GetChannelId(), addr, chdr.GetTxId(), cb.HeaderType_name[chdr.GetType()])

		configSeq, err := runWithTimeout(ctx, bh.validateTimeout(), func() (uint64, error) {
			return processor.ProcessNormalMsg(msg)
		})
		if err != nil {
			if errors2.Is(err, context.DeadlineExceeded) {
				logger.Warningf("[channel: %s] Validation timeout for message from %s", chdr.GetChannelId(), addr)
				return &ab.BroadcastResponse{Status: StatusValidationTimeout, Info: "validation timeout"}, true
			}
			if errors2.Is(err, context.Canceled) {
				logger.Debugf("[channel: %s] Context canceled during validation of message from %s", chdr.GetChannelId(), addr)
				return nil, false
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}, true
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}, true
		}

		_, err = runWithTimeout(ctx, bh.orderTimeout(), func() (any, error) {
			return nil, processor.Order(msg, configSeq)
		})
		if err != nil {
			if errors2.Is(err, context.DeadlineExceeded) {
				logger.Warningf("[channel: %s] Ordering timeout for message from %s", chdr.GetChannelId(), addr)
				return &ab.BroadcastResponse{Status: StatusOrderingTimeout, Info: "ordering timeout"}, true
			}
			if errors2.Is(err, context.Canceled) {
				logger.Debugf("[channel: %s] Context canceled during ordering of message from %s", chdr.GetChannelId(), addr)
				return nil, false
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}, true
		}
	} else { // isConfig
		logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.GetChannelId(), addr)

		result, err := runWithTimeout(ctx, bh.validateTimeout(), func() (configUpdateResult, error) {
			config, configSeq, err := processor.ProcessConfigUpdateMsg(msg)
			return configUpdateResult{config: config, configSeq: configSeq}, err
		})
		if err != nil {
			if errors2.Is(err, context.DeadlineExceeded) {
				logger.Warningf("[channel: %s] Validation timeout for config message from %s", chdr.GetChannelId(), addr)
				return &ab.BroadcastResponse{Status: StatusValidationTimeout, Info: "validation timeout"}, true
			}
			if errors2.Is(err, context.Canceled) {
				logger.Debugf("[channel: %s] Context canceled during validation of config message from %s", chdr.GetChannelId(), addr)
				return nil, false
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}, true
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}, true
		}

		_, err = runWithTimeout(ctx, bh.orderTimeout(), func() (any, error) {
			return nil, processor.Configure(result.config, result.configSeq)
		})
		if err != nil {
			if errors2.Is(err, context.DeadlineExceeded) {
				logger.Warningf("[channel: %s] Ordering timeout for config message from %s", chdr.GetChannelId(), addr)
				return &ab.BroadcastResponse{Status: StatusOrderingTimeout, Info: "ordering timeout"}, true
			}
			if errors2.Is(err, context.Canceled) {
				logger.Debugf("[channel: %s] Context canceled during ordering of config message from %s", chdr.GetChannelId(), addr)
				return nil, false
			}
			logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with SERVICE_UNAVAILABLE: rejected by Configure: %s", chdr.GetChannelId(), addr, err)
			return &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}, true
		}
	}

	logger.Debugf("[channel: %s] Broadcast has successfully enqueued message of type %s from %s", chdr.GetChannelId(), cb.HeaderType_name[chdr.GetType()], addr)

	return &ab.BroadcastResponse{Status: cb.Status_SUCCESS}, true
}

// timeoutResult pairs the result of a blocking operation with its error.
type timeoutResult[T any] struct {
	value T
	err   error
}

// configUpdateResult pairs the outputs of ProcessConfigUpdateMsg.
type configUpdateResult struct {
	config    *cb.Envelope
	configSeq uint64
}

// runWithTimeout runs fn in a goroutine and waits for it to complete, for the
// timeout to expire, or for the context to be canceled. When the timeout
// expires or the context is canceled, the returned error wraps the context
// error. The goroutine running fn is not killed; it is expected to finish on
// its own and send its result to the buffered channel, which is then garbage
// collected.
func runWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func() (T, error)) (T, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	resultCh := make(chan timeoutResult[T], 1)
	go func() {
		v, err := fn()
		resultCh <- timeoutResult[T]{value: v, err: err}
	}()

	select {
	case result := <-resultCh:
		return result.value, result.err
	case <-runCtx.Done():
		var zero T
		return zero, runCtx.Err()
	}
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
