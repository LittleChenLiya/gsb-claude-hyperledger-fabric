/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser_test

import (
	"context"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics/metricsfakes"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset"
	mspproto "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/core/chaincode/lifecycle"
	"github.com/hyperledger/fabric/core/endorser"
	"github.com/hyperledger/fabric/core/endorser/fake"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protoutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

// buildTrackedSignedProposal is a local, fully controlled fixture: it builds a
// syntactically valid SignedProposal for a fixed (nonce, creator) pair so that
// every invocation shares the same transaction ID, while the chaincode args
// and transient data can be varied to produce same/different payloads.
func buildTrackedSignedProposal(channelID, chaincodeName, txID string, input *pb.ChaincodeInput, transient map[string][]byte) *pb.SignedProposal {
	return &pb.SignedProposal{
		ProposalBytes: protoutil.MarshalOrPanic(&pb.Proposal{
			Header: protoutil.MarshalOrPanic(&cb.Header{
				ChannelHeader: protoutil.MarshalOrPanic(&cb.ChannelHeader{
					Type:      int32(cb.HeaderType_ENDORSER_TRANSACTION),
					ChannelId: channelID,
					Extension: protoutil.MarshalOrPanic(&pb.ChaincodeHeaderExtension{
						ChaincodeId: &pb.ChaincodeID{Name: chaincodeName},
					}),
					TxId: txID,
				}),
				SignatureHeader: protoutil.MarshalOrPanic(&cb.SignatureHeader{
					Creator: protoutil.MarshalOrPanic(&mspproto.SerializedIdentity{
						Mspid: "msp-id",
					}),
					Nonce: []byte("nonce"),
				}),
			}),
			Payload: protoutil.MarshalOrPanic(&pb.ChaincodeProposalPayload{
				Input: protoutil.MarshalOrPanic(&pb.ChaincodeInvocationSpec{
					ChaincodeSpec: &pb.ChaincodeSpec{Input: input},
				}),
				TransientMap: transient,
			}),
		}),
		Signature: []byte("signature"),
	}
}

func inputWithArgs(args ...string) *pb.ChaincodeInput {
	in := &pb.ChaincodeInput{}
	for _, a := range args {
		in.Args = append(in.Args, []byte(a))
	}
	return in
}

func newTrackedEndorser(support endorser.Support, tracker *endorser.ProposalFingerprintTracker) *endorser.Endorser {
	fakeChannelMSPIdentityDeserializer := &fake.IdentityDeserializer{}
	fakeIdentity := &fake.Identity{}
	fakeChannelMSPIdentityDeserializer.DeserializeIdentityReturns(fakeIdentity, nil)

	fakeProposalDuration := &metricsfakes.Histogram{}
	fakeProposalDuration.WithReturns(fakeProposalDuration)
	fakeProposalACLCheckFailed := &metricsfakes.Counter{}
	fakeProposalACLCheckFailed.WithReturns(fakeProposalACLCheckFailed)
	fakeInitFailed := &metricsfakes.Counter{}
	fakeInitFailed.WithReturns(fakeInitFailed)
	fakeEndorsementsFailed := &metricsfakes.Counter{}
	fakeEndorsementsFailed.WithReturns(fakeEndorsementsFailed)
	fakeSimulateFailure := &metricsfakes.Counter{}
	fakeSimulateFailure.WithReturns(fakeSimulateFailure)
	fakeDuplicateTxsFailure := &metricsfakes.Counter{}
	fakeDuplicateTxsFailure.WithReturns(fakeDuplicateTxsFailure)

	channelFetcher := &fake.ChannelFetcher{}
	channelFetcher.ChannelReturns(&endorser.Channel{
		IdentityDeserializer: fakeChannelMSPIdentityDeserializer,
	})

	return &endorser.Endorser{
		LocalMSP:               &fake.IdentityDeserializer{},
		PrivateDataDistributor: &fake.PrivateDataDistributor{},
		ChannelFetcher:         channelFetcher,
		Support:                support,
		RequestTracker:         tracker,
		Metrics: &endorser.Metrics{
			ProposalDuration:         fakeProposalDuration,
			ProposalsReceived:        &metricsfakes.Counter{},
			SuccessfulProposals:      &metricsfakes.Counter{},
			ProposalValidationFailed: &metricsfakes.Counter{},
			ProposalACLCheckFailed:   fakeProposalACLCheckFailed,
			InitFailed:               fakeInitFailed,
			EndorsementsFailed:       fakeEndorsementsFailed,
			DuplicateTxsFailure:      fakeDuplicateTxsFailure,
			SimulationFailure:        fakeSimulateFailure,
		},
	}
}

func newTrackingSupport() *fake.Support {
	support := &fake.Support{}
	support.ExecuteReturns(&pb.Response{Status: 200, Payload: []byte("response-payload")}, nil, nil)
	support.ChaincodeEndorsementInfoReturns(&lifecycle.ChaincodeEndorsementInfo{
		Version:           "chaincode-definition-version",
		EndorsementPlugin: "plugin-name",
	}, nil)
	support.EndorseWithPluginReturns(&pb.Endorsement{
		Endorser:  []byte("endorser-identity"),
		Signature: []byte("endorser-signature"),
	}, []byte("endorsement-payload"), nil)

	txSimulator := &fake.TxSimulator{}
	txSimulator.GetTxSimulationResultsReturns(&ledger.TxSimulationResults{
		PubSimulationResults: &rwset.TxReadWriteSet{},
	}, nil)
	support.GetTxSimulatorReturns(txSimulator, nil)
	support.GetHistoryQueryExecutorReturns(&fake.HistoryQueryExecutor{}, nil)
	// Every proposal is treated as not yet committed, so the ledger-level
	// uniqueness check never trips; only the in-process tracker observes
	// repeated transaction IDs.
	support.GetTransactionByIDReturns(nil, errors.New("not committed"))
	// The fixture chaincode is a user chaincode, so the ACL path is exercised.
	support.IsSysCCReturns(false)
	return support
}

var _ = Describe("Endorsement request idempotency tracking", func() {
	const (
		channelID     = "channel-id"
		chaincodeName = "chaincode-name"
		txID          = "6f142589e4ef6a1e62c9c816e2074f70baa9f7cf67c2f0c287d4ef907d6d2015"
	)

	var (
		support *fake.Support
		tracker *endorser.ProposalFingerprintTracker
		e       *endorser.Endorser
	)

	BeforeEach(func() {
		support = newTrackingSupport()
		tracker = endorser.NewProposalFingerprintTracker()
		e = newTrackedEndorser(support, tracker)
	})

	It("records an identical retry of the same transaction id as a replay", func() {
		input := inputWithArgs("move", "a", "b", "10")
		transient := map[string][]byte{"secret": []byte("sauce")}
		prop1 := buildTrackedSignedProposal(channelID, chaincodeName, txID, input, transient)
		prop2 := buildTrackedSignedProposal(channelID, chaincodeName, txID, input, transient)

		resp1, err := e.ProcessProposal(context.Background(), prop1)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp1.GetResponse().GetStatus()).To(Equal(int32(200)))
		Expect(resp1.GetEndorsement()).NotTo(BeNil())

		resp2, err := e.ProcessProposal(context.Background(), prop2)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp2.GetResponse()).To(Equal(resp1.GetResponse()))
		Expect(resp2.GetEndorsement()).To(Equal(resp1.GetEndorsement()))

		// Tracking is read-only and never short-circuits: the chaincode is
		// simulated and endorsed on every request.
		Expect(support.ExecuteCallCount()).To(Equal(2))
		Expect(support.EndorseWithPluginCallCount()).To(Equal(2))
		Expect(support.CheckACLCallCount()).To(Equal(2))

		rec := tracker.Lookup(channelID, txID)
		Expect(rec).NotTo(BeNil())
		Expect(tracker.Len()).To(Equal(1))
		Expect(rec.Requests).To(Equal(2))
		Expect(rec.Replays).To(Equal(1))
		Expect(rec.PayloadMismatches).To(Equal(0))
	})

	It("does not merge requests that share a transaction id but carry different transient data", func() {
		baseInput := inputWithArgs("move", "a", "b", "10")
		prop1 := buildTrackedSignedProposal(channelID, chaincodeName, txID, baseInput, map[string][]byte{
			"secret": []byte("sauce-one"),
		})
		prop2 := buildTrackedSignedProposal(channelID, chaincodeName, txID, baseInput, map[string][]byte{
			"secret": []byte("sauce-two"),
		})

		// Both requests are still fully processed despite the mismatch.
		resp1, err := e.ProcessProposal(context.Background(), prop1)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp1.GetEndorsement()).NotTo(BeNil())
		resp2, err := e.ProcessProposal(context.Background(), prop2)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp2.GetEndorsement()).NotTo(BeNil())

		Expect(support.ExecuteCallCount()).To(Equal(2))
		Expect(support.CheckACLCallCount()).To(Equal(2))

		rec := tracker.Lookup(channelID, txID)
		Expect(rec).NotTo(BeNil())
		Expect(rec.Requests).To(Equal(2))
		Expect(rec.Replays).To(Equal(0))
		Expect(rec.PayloadMismatches).To(Equal(1))

		// The record keeps the fingerprint of the first request, so retries of
		// the second request are counted as mismatches too and never merged.
		firstFingerprint := rec.Fingerprint
		resp3, err := e.ProcessProposal(context.Background(), prop2)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp3.GetEndorsement()).NotTo(BeNil())
		rec = tracker.Lookup(channelID, txID)
		Expect(rec.Requests).To(Equal(3))
		Expect(rec.Replays).To(Equal(0))
		Expect(rec.PayloadMismatches).To(Equal(2))
		Expect(rec.Fingerprint).To(Equal(firstFingerprint))
	})

	It("treats different chaincode args as a payload mismatch", func() {
		prop1 := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move", "a", "b", "10"), nil)
		prop2 := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move", "a", "b", "99"), nil)

		_, err := e.ProcessProposal(context.Background(), prop1)
		Expect(err).NotTo(HaveOccurred())
		_, err = e.ProcessProposal(context.Background(), prop2)
		Expect(err).NotTo(HaveOccurred())

		rec := tracker.Lookup(channelID, txID)
		Expect(rec.Requests).To(Equal(2))
		Expect(rec.Replays).To(Equal(0))
		Expect(rec.PayloadMismatches).To(Equal(1))
	})

	It("scopes records by channel id and transaction id", func() {
		input := inputWithArgs("move", "a", "b", "10")
		prop1 := buildTrackedSignedProposal(channelID, chaincodeName, txID, input, nil)

		_, err := e.ProcessProposal(context.Background(), prop1)
		Expect(err).NotTo(HaveOccurred())

		// The same fingerprint observed for a different (channel, txid) key is
		// a first sighting on that key, not a replay.
		fp := endorser.ComputeProposalFingerprint(input, nil)
		rec, replay, mismatch := tracker.Observe("other-channel", txID, fp, time.Now())
		Expect(rec.Requests).To(Equal(1))
		Expect(replay).To(BeFalse())
		Expect(mismatch).To(BeFalse())
		Expect(tracker.Len()).To(Equal(2))
		Expect(tracker.Lookup("other-channel", txID)).NotTo(BeNil())
	})

	It("does not record a request that fails policy/ACL validation", func() {
		support.CheckACLReturns(errors.New("acl-denied"))
		prop := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move"), nil)

		resp, err := e.ProcessProposal(context.Background(), prop)
		Expect(err).To(MatchError("acl-denied"))
		Expect(resp.GetResponse().GetStatus()).To(Equal(int32(500)))

		// Tracking must not bypass, precede, or record around the policy
		// failure.
		Expect(tracker.Lookup(channelID, txID)).To(BeNil())
		Expect(tracker.Len()).To(Equal(0))
		Expect(support.ExecuteCallCount()).To(Equal(0))
	})

	It("does not record a request that fails signature validation", func() {
		failingDeserializer := &fake.IdentityDeserializer{}
		failingDeserializer.DeserializeIdentityReturns(nil, errors.New("bad-identity"))
		channelFetcher := &fake.ChannelFetcher{}
		channelFetcher.ChannelReturns(&endorser.Channel{IdentityDeserializer: failingDeserializer})

		failingEndorser := &endorser.Endorser{
			LocalMSP:               &fake.IdentityDeserializer{},
			PrivateDataDistributor: &fake.PrivateDataDistributor{},
			ChannelFetcher:         channelFetcher,
			Support:                support,
			RequestTracker:         tracker,
			Metrics:                e.Metrics,
		}

		prop := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move"), nil)
		resp, err := failingEndorser.ProcessProposal(context.Background(), prop)
		Expect(err).To(HaveOccurred())
		Expect(resp.GetResponse().GetStatus()).To(Equal(int32(500)))
		Expect(tracker.Lookup(channelID, txID)).To(BeNil())
		Expect(tracker.Len()).To(Equal(0))
	})

	It("does not treat an already committed transaction as an in-process replay", func() {
		support.GetTransactionByIDReturns(&pb.ProcessedTransaction{}, nil)
		prop := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move"), nil)

		resp, err := e.ProcessProposal(context.Background(), prop)
		Expect(err).To(MatchError(ContainSubstring("duplicate transaction found")))
		Expect(resp.GetResponse().GetStatus()).To(Equal(int32(500)))

		// Committed-tx rejection happens before tracking and before the ACL
		// check, so nothing is recorded and the chaincode never runs.
		Expect(tracker.Lookup(channelID, txID)).To(BeNil())
		Expect(support.CheckACLCallCount()).To(Equal(0))
		Expect(support.ExecuteCallCount()).To(Equal(0))
	})

	It("works without a tracker configured", func() {
		endorserWithoutTracker := newTrackedEndorser(support, nil)
		prop := buildTrackedSignedProposal(channelID, chaincodeName, txID, inputWithArgs("move"), map[string][]byte{"k": []byte("v")})
		resp, err := endorserWithoutTracker.ProcessProposal(context.Background(), prop)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetEndorsement()).NotTo(BeNil())
	})
})

var _ = Describe("Proposal fingerprinting", func() {
	It("is stable regardless of map iteration order", func() {
		input := &pb.ChaincodeInput{
			Args: [][]byte{[]byte("move"), []byte("10")},
			Decorations: map[string][]byte{
				"d1": []byte("v1"),
				"d2": []byte("v2"),
			},
		}
		transient := map[string][]byte{
			"zeta":  []byte("z"),
			"alpha": []byte("a"),
		}
		expected := endorser.ComputeProposalFingerprint(input, transient)
		for i := 0; i < 10; i++ {
			Expect(endorser.ComputeProposalFingerprint(input, transient)).To(Equal(expected))
		}
	})

	It("differs for different transient values, keys, or args", func() {
		input := inputWithArgs("move", "10")
		base := endorser.ComputeProposalFingerprint(input, map[string][]byte{"k": []byte("v")})

		Expect(endorser.ComputeProposalFingerprint(input, map[string][]byte{"k": []byte("other")})).NotTo(Equal(base))
		Expect(endorser.ComputeProposalFingerprint(input, map[string][]byte{"other": []byte("v")})).NotTo(Equal(base))
		Expect(endorser.ComputeProposalFingerprint(input, nil)).NotTo(Equal(base))
		Expect(endorser.ComputeProposalFingerprint(inputWithArgs("move", "11"), map[string][]byte{"k": []byte("v")})).NotTo(Equal(base))
	})
})
