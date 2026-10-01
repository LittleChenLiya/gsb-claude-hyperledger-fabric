/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"bytes"
	"slices"

	"google.golang.org/protobuf/proto"
)

// MembershipDiff is the read-only difference between two membership views.
// It partitions the members into three disjoint sets:
//   - Joined: members present in the new view but absent in the old view
//   - Left: members present in the old view but absent in the new view
//   - Updated: members present in both views whose membership information changed
//
// Members are matched by their PKI-ID. Duplicate entries for the same member
// are collapsed into a single entry, so duplicate messages for the same
// member produce no diff. The returned slices are stably sorted by member
// identifier (PKI-ID).
type MembershipDiff struct {
	joined  Members
	left    Members
	updated Members
}

// Joined returns the members that joined between the two views.
func (d MembershipDiff) Joined() Members { return d.joined }

// Left returns the members that left between the two views.
func (d MembershipDiff) Left() Members { return d.left }

// Updated returns the members that were updated between the two views.
func (d MembershipDiff) Updated() Members { return d.updated }

// IsEmpty reports whether the diff contains no joins, leaves or updates.
func (d MembershipDiff) IsEmpty() bool {
	return len(d.joined) == 0 && len(d.left) == 0 && len(d.updated) == 0
}

// Diff computes the read-only difference between two membership views.
// The oldView and newView arguments are not modified.
//
// Members are matched by their PKI-ID. A member that appears in the new view
// but not in the old view is reported as joined. A member that appears in
// the old view but not in the new view is reported as left. A member that
// appears in both views but whose membership information changed is reported
// as updated.
//
// Duplicate entries for the same member are collapsed into a single entry,
// so duplicate messages for the same member produce no diff. The returned
// slices are stably sorted by member identifier (PKI-ID).
func Diff(oldView, newView Members) MembershipDiff {
	oldByID := indexByID(oldView)
	newByID := indexByID(newView)

	diff := MembershipDiff{}

	for id, newMember := range newByID {
		oldMember, exists := oldByID[id]
		if !exists {
			diff.joined = append(diff.joined, newMember)
			continue
		}
		if !membersEqual(oldMember, newMember) {
			diff.updated = append(diff.updated, newMember)
		}
	}

	for id, oldMember := range oldByID {
		if _, exists := newByID[id]; !exists {
			diff.left = append(diff.left, oldMember)
		}
	}

	sortByPKIid(diff.joined)
	sortByPKIid(diff.left)
	sortByPKIid(diff.updated)

	return diff
}

// indexByID collapses the members into a map keyed by their string-encoded
// PKI-ID. Duplicate entries for the same member are collapsed into a single
// entry (the last occurrence wins), so duplicate messages for the same
// member produce no diff.
func indexByID(members Members) map[string]NetworkMember {
	res := make(map[string]NetworkMember, len(members))
	for _, member := range members {
		res[string(member.PKIid)] = member
	}
	return res
}

// membersEqual reports whether two members have the same membership
// information. The signed envelope is not compared because it is a
// transport-level artifact that changes on every alive message.
func membersEqual(a, b NetworkMember) bool {
	return a.Endpoint == b.Endpoint &&
		bytes.Equal(a.Metadata, b.Metadata) &&
		a.InternalEndpoint == b.InternalEndpoint &&
		proto.Equal(a.Properties, b.Properties)
}

// sortByPKIid sorts the members stably by their PKI-ID.
func sortByPKIid(members Members) {
	slices.SortStableFunc(members, func(a, b NetworkMember) int {
		return bytes.Compare(a.PKIid, b.PKIid)
	})
}
