/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

// ProposalFingerprint is a read-only fingerprint of the chaincode invocation
// carried by a proposal. Unlike the proposal hash (which deliberately excludes
// the transient data, see UnpackProposal), the fingerprint covers both the
// chaincode input and the transient map, so that two endorsement requests that
// share a transaction ID but carry different payloads cannot be confused.
type ProposalFingerprint [sha256.Size]byte

func (f ProposalFingerprint) String() string {
	return hex.EncodeToString(f[:])
}

// RequestRecord is the read-only bookkeeping kept for a single
// (channel ID, transaction ID) pair that passed proposal validation.
type RequestRecord struct {
	ChannelID   string
	TxID        string
	Fingerprint ProposalFingerprint
	FirstSeen   time.Time
	LastSeen    time.Time
	// Requests is the number of endorsement requests seen for this key.
	Requests int
	// Replays is the number of requests that carried an identical fingerprint.
	Replays int
	// PayloadMismatches is the number of requests that carried a different
	// fingerprint than the first request for this key. Such a request is never
	// merged into the record of the original request.
	PayloadMismatches int
}

// ProposalFingerprintTracker keeps an in-memory, read-only trail of
// endorsement requests keyed by channel ID and transaction ID. It is used to
// trace idempotent retries of endorsement requests: it never short-circuits
// policy validation, simulation or endorsement, it only observes.
type ProposalFingerprintTracker struct {
	mu      sync.Mutex
	records map[string]*RequestRecord
}

// NewProposalFingerprintTracker returns an initialized tracker.
func NewProposalFingerprintTracker() *ProposalFingerprintTracker {
	return &ProposalFingerprintTracker{records: map[string]*RequestRecord{}}
}

func trackerKey(channelID, txID string) string {
	return channelID + "\x00" + txID
}

// ComputeProposalFingerprint derives a stable fingerprint of a chaincode
// invocation. The chaincode input is marshaled deterministically and the
// transient map entries are folded in under their lexicographically sorted
// keys, ensuring that different transient data always yields a different
// fingerprint while equal payloads always yield the same fingerprint.
func ComputeProposalFingerprint(input *pb.ChaincodeInput, transient map[string][]byte) ProposalFingerprint {
	hasher := sha256.New()

	if input != nil {
		// Deterministic marshaling makes the result independent of map
		// iteration order (e.g. for Decorations).
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
		if err != nil {
			// ChaincodeInput only contains scalar, byte-slice and
			// map[string][]byte fields, so a deterministic marshal cannot
			// realistically fail. Fall back to folding in the raw args rather
			// than panicking on the request path.
			for _, arg := range input.GetArgs() {
				hasher.Write(arg)
			}
		} else {
			hasher.Write(b)
		}
	}

	// Sort transient keys explicitly; a map's iteration order is randomized.
	keys := make([]string, 0, len(transient))
	for k := range transient {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		hasher.Write([]byte(k))
		hasher.Write([]byte{0})
		hasher.Write(transient[k])
	}

	var fp ProposalFingerprint
	copy(fp[:], hasher.Sum(nil))
	return fp
}

// Observe records an endorsement request that has already passed validation
// and ACL checks. It returns the (updated) record and the outcome of the
// observation. Observe never returns an error and never affects request
// processing: its return values are for observability only.
//
// The first request for a (channelID, txID) creates the record. Subsequent
// requests with the same fingerprint are counted as replays, while requests
// with a different fingerprint are counted as payload mismatches and are not
// merged into the original record.
func (t *ProposalFingerprintTracker) Observe(channelID, txID string, fingerprint ProposalFingerprint, at time.Time) (record *RequestRecord, replay bool, mismatch bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := trackerKey(channelID, txID)
	rec, ok := t.records[key]
	if !ok {
		rec = &RequestRecord{
			ChannelID:   channelID,
			TxID:        txID,
			Fingerprint: fingerprint,
			FirstSeen:   at,
			LastSeen:    at,
			Requests:    1,
		}
		t.records[key] = rec
		return rec, false, false
	}

	rec.Requests++
	rec.LastSeen = at
	switch {
	case rec.Fingerprint == fingerprint:
		rec.Replays++
		return rec, true, false
	default:
		// The request shares the transaction ID but carries different
		// payload/transient data. Keep the original record untouched apart from
		// counting the mismatch, so the two requests can never be merged.
		rec.PayloadMismatches++
		return rec, false, true
	}
}

// Lookup returns a copy of the record for the given channel ID and
// transaction ID, or nil if no request has been observed for the key.
func (t *ProposalFingerprintTracker) Lookup(channelID, txID string) *RequestRecord {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.records[trackerKey(channelID, txID)]
	if !ok {
		return nil
	}
	cp := *rec
	return &cp
}

// Len returns the number of distinct (channel ID, transaction ID) records.
func (t *ProposalFingerprintTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.records)
}
