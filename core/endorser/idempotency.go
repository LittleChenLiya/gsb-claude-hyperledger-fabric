/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"bytes"
	"crypto/sha256"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/pkg/errors"
)

// RequestFingerprint is a collision-resistant digest of an endorsement request.
// Unlike the transaction's proposal hash (which deliberately excludes the
// transient data), the fingerprint covers the full proposal payload so that two
// requests carrying the same transaction ID but different transient data produce
// different fingerprints. The fingerprint is read-only: it is computed from the
// proposal bytes and never modifies the proposal.
type RequestFingerprint []byte

// FingerprintRequest computes the read-only fingerprint of a signed proposal.
// The fingerprint covers the serialized proposal bytes (the channel header, the
// signature header and the chaincode proposal payload, including the transient
// map); it never modifies the proposal.
func FingerprintRequest(signedProp *peer.SignedProposal) RequestFingerprint {
	fp := sha256.Sum256(signedProp.GetProposalBytes())
	return fp[:]
}

// ErrIdempotencyConflict is returned when a request reuses a transaction ID that
// was already seen with a different payload (for example, different transient
// data). Such requests must not be merged with the original one.
var ErrIdempotencyConflict = errors.New("idempotency conflict: transaction ID was already used with a different payload")

// IdempotencyTracker records the fingerprints of endorsement requests keyed by
// channel and transaction ID so that repeated requests can be distinguished from
// conflicting requests.
//
// The tracker is read-only with respect to the proposal: it never alters the
// request and never short-circuits policy validation. The endorser always runs
// the ACL check for application chaincodes, even when a request is recognized as
// a replay.
type IdempotencyTracker struct {
	mutex        sync.RWMutex
	fingerprints map[string]map[string]RequestFingerprint
}

// NewIdempotencyTracker returns an initialized IdempotencyTracker.
func NewIdempotencyTracker() *IdempotencyTracker {
	return &IdempotencyTracker{
		fingerprints: make(map[string]map[string]RequestFingerprint),
	}
}

// CheckAndRecord looks up the fingerprint recorded for (channelID, txID).
//
//   - If no fingerprint has been recorded, fp is recorded and replay is false.
//   - If the same fingerprint is recorded, replay is true, indicating a benign
//     retry of an identical request.
//   - If a different fingerprint is recorded, a conflict error is returned and
//     the new fingerprint is not recorded, because the two requests must not be
//     merged.
//
// The method is safe for concurrent use.
func (t *IdempotencyTracker) CheckAndRecord(channelID, txID string, fp RequestFingerprint) (replay bool, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	byChannel, ok := t.fingerprints[channelID]
	if !ok {
		byChannel = make(map[string]RequestFingerprint)
		t.fingerprints[channelID] = byChannel
	}

	if recorded, ok := byChannel[txID]; ok {
		if !bytes.Equal(recorded, fp) {
			return false, errors.Wrapf(ErrIdempotencyConflict, "channel [%s] txID [%s]", channelID, txID)
		}
		return true, nil
	}

	byChannel[txID] = fp
	return false, nil
}
