/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"fmt"
	"testing"
	"time"

	proto "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/stretchr/testify/require"
)

// TestDiffEmptyViews verifies that diffing two empty views yields an empty diff.
func TestDiffEmptyViews(t *testing.T) {
	diff := Diff(Members{}, Members{})
	require.True(t, diff.IsEmpty())
	require.Empty(t, diff.Joined())
	require.Empty(t, diff.Left())
	require.Empty(t, diff.Updated())
}

// TestDiffIdenticalViews verifies that diffing two identical views yields an empty diff.
func TestDiffIdenticalViews(t *testing.T) {
	view := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("m1")},
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002", Metadata: []byte("m2")},
	}
	diff := Diff(view, view)
	require.True(t, diff.IsEmpty())
	require.Empty(t, diff.Joined())
	require.Empty(t, diff.Left())
	require.Empty(t, diff.Updated())
}

// TestDiffJoins verifies that members present only in the new view are reported as joined.
func TestDiffJoins(t *testing.T) {
	oldView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001"},
	}
	newView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001"},
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002"},
		{PKIid: common.PKIidType("p3"), Endpoint: "localhost:1003"},
	}
	diff := Diff(oldView, newView)
	require.False(t, diff.IsEmpty())
	require.Empty(t, diff.Left())
	require.Empty(t, diff.Updated())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002"},
		{PKIid: common.PKIidType("p3"), Endpoint: "localhost:1003"},
	}, diff.Joined())
}

// TestDiffLeaves verifies that members present only in the old view are reported as left.
func TestDiffLeaves(t *testing.T) {
	oldView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001"},
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002"},
		{PKIid: common.PKIidType("p3"), Endpoint: "localhost:1003"},
	}
	newView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001"},
	}
	diff := Diff(oldView, newView)
	require.False(t, diff.IsEmpty())
	require.Empty(t, diff.Joined())
	require.Empty(t, diff.Updated())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002"},
		{PKIid: common.PKIidType("p3"), Endpoint: "localhost:1003"},
	}, diff.Left())
}

// TestDiffUpdates verifies that members present in both views but with changed
// membership information are reported as updated.
func TestDiffUpdates(t *testing.T) {
	oldView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("v1")},
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002", Metadata: []byte("v2")},
	}
	newView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("v1-updated")},
		{PKIid: common.PKIidType("p2"), Endpoint: "localhost:1002", Metadata: []byte("v2")},
	}
	diff := Diff(oldView, newView)
	require.False(t, diff.IsEmpty())
	require.Empty(t, diff.Joined())
	require.Empty(t, diff.Left())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("v1-updated")},
	}, diff.Updated())
}

// TestDiffDuplicatesProduceNoDiff verifies that duplicate entries for the same
// member are collapsed and produce no diff.
func TestDiffDuplicatesProduceNoDiff(t *testing.T) {
	view := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("m1")},
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("m1")},
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Metadata: []byte("m1")},
	}
	diff := Diff(view, view)
	require.True(t, diff.IsEmpty())
}

// TestDiffStableSort verifies that the diff output is stably sorted by member
// identifier (PKI-ID).
func TestDiffStableSort(t *testing.T) {
	oldView := Members{
		{PKIid: common.PKIidType("z"), Endpoint: "localhost:1"},
		{PKIid: common.PKIidType("a"), Endpoint: "localhost:2"},
	}
	newView := Members{
		{PKIid: common.PKIidType("m"), Endpoint: "localhost:3"},
		{PKIid: common.PKIidType("b"), Endpoint: "localhost:4"},
	}
	diff := Diff(oldView, newView)
	require.Equal(t, Members{
		{PKIid: common.PKIidType("b"), Endpoint: "localhost:4"},
		{PKIid: common.PKIidType("m"), Endpoint: "localhost:3"},
	}, diff.Joined())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("a"), Endpoint: "localhost:2"},
		{PKIid: common.PKIidType("z"), Endpoint: "localhost:1"},
	}, diff.Left())
}

// TestDiffEnvelopeNotCompared verifies that the signed envelope is not compared,
// so members with the same membership information but different envelopes are
// not reported as updated.
func TestDiffEnvelopeNotCompared(t *testing.T) {
	env1 := &proto.Envelope{Payload: []byte("payload1")}
	env2 := &proto.Envelope{Payload: []byte("payload2")}
	oldView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Envelope: env1},
	}
	newView := Members{
		{PKIid: common.PKIidType("p1"), Endpoint: "localhost:1001", Envelope: env2},
	}
	diff := Diff(oldView, newView)
	require.True(t, diff.IsEmpty(), "envelope should not be compared")
}

// TestDiffAllCategories verifies that joins, leaves and updates are all
// reported correctly in a single diff.
func TestDiffAllCategories(t *testing.T) {
	oldView := Members{
		{PKIid: common.PKIidType("a"), Endpoint: "localhost:1", Metadata: []byte("a-old")},
		{PKIid: common.PKIidType("b"), Endpoint: "localhost:2", Metadata: []byte("b")},
		{PKIid: common.PKIidType("c"), Endpoint: "localhost:3", Metadata: []byte("c")},
	}
	newView := Members{
		{PKIid: common.PKIidType("a"), Endpoint: "localhost:1", Metadata: []byte("a-new")},
		{PKIid: common.PKIidType("c"), Endpoint: "localhost:3", Metadata: []byte("c")},
		{PKIid: common.PKIidType("d"), Endpoint: "localhost:4", Metadata: []byte("d")},
	}
	diff := Diff(oldView, newView)
	require.False(t, diff.IsEmpty())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("d"), Endpoint: "localhost:4", Metadata: []byte("d")},
	}, diff.Joined())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("b"), Endpoint: "localhost:2", Metadata: []byte("b")},
	}, diff.Left())
	require.Equal(t, Members{
		{PKIid: common.PKIidType("a"), Endpoint: "localhost:1", Metadata: []byte("a-new")},
	}, diff.Updated())
}

// requireMembersEqual asserts that the actual members match the expected members,
// ignoring the Envelope field (which is a transport-level artifact).
func requireMembersEqual(t *testing.T, expected, actual Members) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.Equal(t, expected[i].PKIid, actual[i].PKIid, "PKI-ID mismatch at index %d", i)
		require.Equal(t, expected[i].Endpoint, actual[i].Endpoint, "Endpoint mismatch at index %d", i)
		require.Equal(t, expected[i].Metadata, actual[i].Metadata, "Metadata mismatch at index %d", i)
	}
}

// createSignedAliveMsg creates a signed alive message for testing.
func createSignedAliveMsg(pkiID string, endpoint string, metadata []byte, incNum uint64, seqNum uint64) *protoext.SignedGossipMessage {
	msg := &proto.GossipMessage{
		Tag: proto.GossipMessage_EMPTY,
		Content: &proto.GossipMessage_AliveMsg{
			AliveMsg: &proto.AliveMessage{
				Membership: &proto.Member{
					Endpoint: endpoint,
					Metadata: metadata,
					PkiId:    []byte(pkiID),
				},
				Timestamp: &proto.PeerTime{
					IncNum: incNum,
					SeqNum: seqNum,
				},
			},
		},
	}
	signedMsg, err := protoext.NoopSign(msg)
	if err != nil {
		panic(err)
	}
	return signedMsg
}

// TestMembershipDiffWithFakeGossipEvents is an integration test that injects
// fake gossip events (out-of-order and duplicate) into the discovery layer
// and verifies the diff between the membership views before and after.
//
// The scenario:
//  1. Initial view: members A and B are alive.
//  2. Inject events:
//     - duplicate of A (same timestamp) -> no change
//     - out-of-order old message for A (seqNum 0 after seqNum 1) -> ignored
//     - update A (newer timestamp) -> A is updated
//     - duplicate of updated A -> no change
//     - new member C joins
//     - member B expires (leaves)
//  3. Final view: A (updated) and C are alive; B left.
//  4. Verify the diff: joined=[C], left=[B], updated=[A].
func TestMembershipDiffWithFakeGossipEvents(t *testing.T) {
	inst := createDiscoveryInstanceWithNoGossip(33900, "d0", []string{})
	defer inst.Stop()

	impl := inst.discoveryImpl()

	incNum := uint64(time.Now().UnixNano())

	// Step 1: Inject initial members A and B.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA"), incNum, 1))
	impl.handleAliveMessage(createSignedAliveMsg("B", "localhost:1002", []byte("metaB"), incNum, 1))

	view1 := impl.GetMembership()
	require.Len(t, view1, 2)

	// Step 2: Inject fake gossip events.
	// Duplicate of A (same timestamp) -> no change.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA"), incNum, 1))
	// Out-of-order old message for A (seqNum 0 after seqNum 1) -> ignored.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA-old"), incNum, 0))
	// Update A (newer timestamp) -> A is updated.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA-updated"), incNum, 2))
	// Duplicate of updated A -> no change.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA-updated"), incNum, 2))
	// New member C joins.
	impl.handleAliveMessage(createSignedAliveMsg("C", "localhost:1003", []byte("metaC"), incNum, 1))
	// Member B expires (leaves).
	impl.expireDeadMembers([]common.PKIidType{[]byte("B")})

	view2 := impl.GetMembership()
	require.Len(t, view2, 2)

	// Step 3: Compute the diff.
	diff := Diff(Members(view1), Members(view2))

	// Step 4: Verify the diff.
	require.False(t, diff.IsEmpty())

	// C joined.
	requireMembersEqual(t, Members{
		{PKIid: common.PKIidType("C"), Endpoint: "localhost:1003", Metadata: []byte("metaC")},
	}, diff.Joined())

	// B left.
	requireMembersEqual(t, Members{
		{PKIid: common.PKIidType("B"), Endpoint: "localhost:1002", Metadata: []byte("metaB")},
	}, diff.Left())

	// A updated.
	requireMembersEqual(t, Members{
		{PKIid: common.PKIidType("A"), Endpoint: "localhost:1001", Metadata: []byte("metaA-updated")},
	}, diff.Updated())

	// Verify the diff is stable sorted by PKI-ID.
	for _, members := range []Members{diff.Joined(), diff.Left(), diff.Updated()} {
		for i := 1; i < len(members); i++ {
			require.True(t, string(members[i-1].PKIid) < string(members[i].PKIid),
				"members should be sorted by PKI-ID")
		}
	}

	// Print the diff for visual inspection.
	fmt.Println("=== Membership Diff ===")
	fmt.Printf("Joined:  %v\n", diff.Joined())
	fmt.Printf("Left:    %v\n", diff.Left())
	fmt.Printf("Updated: %v\n", diff.Updated())
	fmt.Println("=======================")
}

// TestMembershipDiffNoChangeOnDuplicates verifies that injecting only duplicate
// messages produces no diff.
func TestMembershipDiffNoChangeOnDuplicates(t *testing.T) {
	inst := createDiscoveryInstanceWithNoGossip(33901, "d0", []string{})
	defer inst.Stop()

	impl := inst.discoveryImpl()

	incNum := uint64(time.Now().UnixNano())

	// Inject initial member A.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA"), incNum, 1))
	view1 := impl.GetMembership()

	// Inject only duplicate messages for A.
	for i := 0; i < 5; i++ {
		impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA"), incNum, 1))
	}
	view2 := impl.GetMembership()

	diff := Diff(Members(view1), Members(view2))
	require.True(t, diff.IsEmpty(), "duplicate messages should produce no diff")
}

// TestMembershipDiffOutOfOrderIgnored verifies that out-of-order (older) messages
// do not regress the membership state.
func TestMembershipDiffOutOfOrderIgnored(t *testing.T) {
	inst := createDiscoveryInstanceWithNoGossip(33902, "d0", []string{})
	defer inst.Stop()

	impl := inst.discoveryImpl()

	incNum := uint64(time.Now().UnixNano())

	// Inject initial member A with seqNum 1.
	impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA"), incNum, 1))
	view1 := impl.GetMembership()

	// Inject out-of-order older messages (seqNum 0) after seqNum 1.
	for i := 0; i < 3; i++ {
		impl.handleAliveMessage(createSignedAliveMsg("A", "localhost:1001", []byte("metaA-old"), incNum, 0))
	}
	view2 := impl.GetMembership()

	diff := Diff(Members(view1), Members(view2))
	require.True(t, diff.IsEmpty(), "out-of-order messages should not regress the state")

	// Verify the member still has the original metadata.
	require.Len(t, view2, 1)
	require.Equal(t, []byte("metaA"), view2[0].Metadata)
}
