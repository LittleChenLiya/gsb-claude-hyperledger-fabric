/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"crypto/sha256"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/stretchr/testify/require"
)

func TestFingerprintRequest(t *testing.T) {
	proposalBytes := []byte("the-proposal-bytes")
	signedProp := &peer.SignedProposal{ProposalBytes: proposalBytes, Signature: []byte("sig")}

	fp := FingerprintRequest(signedProp)

	expected := sha256.Sum256(proposalBytes)
	require.Equal(t, expected[:], []byte(fp))

	// the fingerprint must be read-only: it must not depend on the signature
	signedPropSameContent := &peer.SignedProposal{ProposalBytes: proposalBytes, Signature: []byte("other-sig")}
	require.Equal(t, fp, FingerprintRequest(signedPropSameContent))

	// different proposal bytes (e.g. different transient data) produce a different fingerprint
	different := &peer.SignedProposal{ProposalBytes: []byte("different-proposal-bytes")}
	require.NotEqual(t, fp, FingerprintRequest(different))
}

func TestIdempotencyTrackerCheckAndRecord(t *testing.T) {
	tracker := NewIdempotencyTracker()

	fpA := RequestFingerprint("fingerprint-a")
	fpB := RequestFingerprint("fingerprint-b")

	// first request for (channel, txID) is recorded and is not a replay
	replay, err := tracker.CheckAndRecord("channel-1", "tx-1", fpA)
	require.NoError(t, err)
	require.False(t, replay)

	// identical request is a benign replay
	replay, err = tracker.CheckAndRecord("channel-1", "tx-1", fpA)
	require.NoError(t, err)
	require.True(t, replay)

	// a request that reuses the txID with a different payload is a conflict and must not merge
	_, err = tracker.CheckAndRecord("channel-1", "tx-1", fpB)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	// the conflict must not overwrite the originally recorded fingerprint
	replay, err = tracker.CheckAndRecord("channel-1", "tx-1", fpA)
	require.NoError(t, err)
	require.True(t, replay)

	// a different channel is tracked independently
	replay, err = tracker.CheckAndRecord("channel-2", "tx-1", fpA)
	require.NoError(t, err)
	require.False(t, replay)
}

func TestIdempotencyTrackerConcurrent(t *testing.T) {
	tracker := NewIdempotencyTracker()

	fpA := RequestFingerprint("fingerprint-a")
	fpB := RequestFingerprint("fingerprint-b")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, _ = tracker.CheckAndRecord("channel", "tx", fpA)
			} else {
				_, _ = tracker.CheckAndRecord("channel", "tx", fpB)
			}
		}(i)
	}
	wg.Wait()

	// after the concurrent burst, the recorded fingerprint must be exactly one of the two
	recorded := tracker.fingerprints["channel"]["tx"]
	require.True(t, string(recorded) == string(fpA) || string(recorded) == string(fpB))
}
