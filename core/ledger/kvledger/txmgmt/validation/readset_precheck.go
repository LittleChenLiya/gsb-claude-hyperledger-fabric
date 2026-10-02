/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validation

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
)

// precheckBatch accumulates the keys written by preceding valid transactions in the
// candidate block. It mirrors the in-block conflict semantics of the regular MVCC
// validation but is never applied to the state database or committed in any other way.
type precheckBatch struct {
	publicWrites map[string]map[string]struct{}
	hashWrites   map[string]map[string]map[string]struct{}
}

func newPrecheckBatch() *precheckBatch {
	return &precheckBatch{
		publicWrites: map[string]map[string]struct{}{},
		hashWrites:   map[string]map[string]map[string]struct{}{},
	}
}

func (b *precheckBatch) publicWriteExists(ns, key string) bool {
	_, ok := b.publicWrites[ns][key]
	return ok
}

func (b *precheckBatch) hashWriteExists(ns, coll string, keyHash []byte) bool {
	_, ok := b.hashWrites[ns][coll][string(keyHash)]
	return ok
}

func (b *precheckBatch) addWrites(txRWSet *rwsetutil.TxRwSet) {
	for _, nsRWSet := range txRWSet.NsRwSets {
		ns := nsRWSet.NameSpace
		for _, kvWrite := range nsRWSet.KvRwSet.GetWrites() {
			if _, ok := b.publicWrites[ns]; !ok {
				b.publicWrites[ns] = map[string]struct{}{}
			}
			b.publicWrites[ns][kvWrite.GetKey()] = struct{}{}
		}
		for _, collHashedRWSet := range nsRWSet.CollHashedRwSets {
			coll := collHashedRWSet.CollectionName
			if _, ok := b.hashWrites[ns]; !ok {
				b.hashWrites[ns] = map[string]map[string]struct{}{}
			}
			if _, ok := b.hashWrites[ns][coll]; !ok {
				b.hashWrites[ns][coll] = map[string]struct{}{}
			}
			for _, hashedWrite := range collHashedRWSet.HashedRwSet.GetHashedWrites() {
				b.hashWrites[ns][coll][string(hashedWrite.GetKeyHash())] = struct{}{}
			}
		}
	}
}

// precheckValidator detects read-set conflicts against the committed state database and
// the writes accumulated in a precheckBatch. It implements a read-only subset of validator.
type precheckValidator struct {
	db *privacyenabledstate.DB
}

func (v *precheckValidator) validateTx(txRWSet *rwsetutil.TxRwSet, batch *precheckBatch) (*ledger.ReadSetConflict, error) {
	for _, nsRWSet := range txRWSet.NsRwSets {
		ns := nsRWSet.NameSpace
		for _, kvRead := range nsRWSet.KvRwSet.GetReads() {
			conflict, err := v.checkKVRead(ns, kvRead, batch)
			if err != nil || conflict != nil {
				return conflict, err
			}
		}
		for _, rqi := range nsRWSet.KvRwSet.GetRangeQueriesInfo() {
			conflict, err := v.checkRangeQuery(ns, rqi)
			if err != nil || conflict != nil {
				return conflict, err
			}
		}
		for _, collHashedRWSet := range nsRWSet.CollHashedRwSets {
			coll := collHashedRWSet.CollectionName
			for _, kvReadHash := range collHashedRWSet.HashedRwSet.GetHashedReads() {
				conflict, err := v.checkKVReadHash(ns, coll, kvReadHash, batch)
				if err != nil || conflict != nil {
					return conflict, err
				}
			}
		}
	}
	return nil, nil
}

func heightComponents(h *version.Height) (uint64, uint64) {
	if h == nil {
		return 0, 0
	}
	return h.BlockNum, h.TxNum
}

func newKVReadConflict(
	ns, key string,
	readVersion, committedVersion *version.Height,
) *ledger.ReadSetConflict {
	readBlockNum, readTxNum := heightComponents(readVersion)
	committedBlockNum, committedTxNum := heightComponents(committedVersion)
	return &ledger.ReadSetConflict{
		Type:              ledger.MVCCReadConflict,
		Namespace:         ns,
		Key:               key,
		ReadBlockNum:      readBlockNum,
		ReadTxNum:         readTxNum,
		CommittedBlockNum: committedBlockNum,
		CommittedTxNum:    committedTxNum,
	}
}

func (v *precheckValidator) checkKVRead(
	ns string,
	kvRead *kvrwset.KVRead,
	batch *precheckBatch,
) (*ledger.ReadSetConflict, error) {
	readVersion := rwsetutil.NewVersion(kvRead.GetVersion())
	committedVersion, err := v.db.GetVersion(ns, kvRead.GetKey())
	if err != nil {
		return nil, err
	}
	if batch.publicWriteExists(ns, kvRead.GetKey()) || !version.AreSame(committedVersion, readVersion) {
		// Either a preceding valid transaction in the candidate block writes this key, or
		// the committed version differs from the version observed by the transaction.
		return newKVReadConflict(ns, kvRead.GetKey(), readVersion, committedVersion), nil
	}
	return nil, nil
}

func (v *precheckValidator) checkKVReadHash(
	ns, coll string,
	kvReadHash *kvrwset.KVReadHash,
	batch *precheckBatch,
) (*ledger.ReadSetConflict, error) {
	readVersion := rwsetutil.NewVersion(kvReadHash.GetVersion())
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.GetKeyHash())
	if err != nil {
		return nil, err
	}
	if batch.hashWriteExists(ns, coll, kvReadHash.GetKeyHash()) || !version.AreSame(committedVersion, readVersion) {
		return newKVReadConflict(ns, "", readVersion, committedVersion), nil
	}
	return nil, nil
}

func (v *precheckValidator) checkRangeQuery(ns string, rqi *kvrwset.RangeQueryInfo) (*ledger.ReadSetConflict, error) {
	combinedItr, err := newCombinedIterator(
		v.db,
		statedb.NewUpdateBatch(),
		ns,
		rqi.GetStartKey(),
		rqi.GetEndKey(),
		!rqi.GetItrExhausted(),
	)
	if err != nil {
		return nil, err
	}
	defer combinedItr.Close()

	var qv rangeQueryValidator
	if rqi.GetReadsMerkleHashes() != nil {
		qv = &rangeQueryHashValidator{}
	} else {
		qv = &rangeQueryResultsValidator{}
	}
	if err := qv.init(rqi, combinedItr); err != nil {
		return nil, err
	}
	valid, err := qv.validate()
	if err != nil || valid {
		return nil, err
	}
	return &ledger.ReadSetConflict{
		Type:      ledger.PhantomReadConflict,
		Namespace: ns,
		StartKey:  rqi.GetStartKey(),
		EndKey:    rqi.GetEndKey(),
	}, nil
}

// CheckReadSetConflicts performs a read-only MVCC validation of the read-sets of the
// endorser transactions in the block against the currently committed state. Unlike the
// regular ValidateAndPrepareBatch, this function never mutates the supplied block or
// its metadata, never invokes state listeners, and never prepares an update batch.
// Hence, it is safe to call before a commit without any effect on the ledger. The actual
// commit must still re-run the full validation as the state may change between the
// precheck and the commit (TOCTOU).
func CheckReadSetConflicts(db *privacyenabledstate.DB, block *common.Block) (*ledger.ReadSetPrecheckReport, error) {
	report := &ledger.ReadSetPrecheckReport{
		BlockNum:  block.GetHeader().GetNumber(),
		TxResults: []*ledger.TxReadSetPrecheckResult{},
	}

	// Work on a private copy of the validation flags so the block of the caller is never mutated.
	txsFilter := txflags.ValidationFlags(append(
		txflags.ValidationFlags(nil),
		block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER]...,
	))

	batch := newPrecheckBatch()
	readValidator := &precheckValidator{db: db}

	for txIndex, envBytes := range block.GetData().GetData() {
		if txsFilter.IsInvalid(txIndex) {
			// The committer already marked the transaction invalid; it is not a read-set conflict.
			continue
		}
		var chdr *common.ChannelHeader
		if len(envBytes) != 0 {
			env, err := protoutil.GetEnvelopeFromBlock(envBytes)
			if err != nil {
				return nil, err
			}
			payload, err := protoutil.UnmarshalPayload(env.GetPayload())
			if err != nil {
				return nil, err
			}
			chdr, err = protoutil.UnmarshalChannelHeader(payload.GetHeader().GetChannelHeader())
			if err != nil {
				return nil, err
			}
		}

		txResult := &ledger.TxReadSetPrecheckResult{
			TxIndexInBlock: uint64(txIndex),
			TxID:           chdr.GetTxId(),
		}

		if common.HeaderType(chdr.GetType()) != common.HeaderType_ENDORSER_TRANSACTION {
			// Only endorser transactions carry an MVCC read-set.
			report.TxResults = append(report.TxResults, txResult)
			continue
		}

		respPayload, err := protoutil.GetActionFromEnvelope(envBytes)
		if err != nil {
			txsFilter.SetFlag(txIndex, peer.TxValidationCode_NIL_TXACTION)
			continue
		}
		txRWSet := &rwsetutil.TxRwSet{}
		if err := txRWSet.FromProtoBytes(respPayload.GetResults()); err != nil {
			txsFilter.SetFlag(txIndex, peer.TxValidationCode_INVALID_OTHER_REASON)
			continue
		}

		conflict, err := readValidator.validateTx(txRWSet, batch)
		if err != nil {
			return nil, err
		}
		txResult.Conflict = conflict
		report.TxResults = append(report.TxResults, txResult)

		// A conflicting transaction would be invalid at commit time and its writes must not
		// affect the validation of the subsequent transactions in this block.
		if conflict == nil {
			batch.addWrites(txRWSet)
		}
	}
	return report, nil
}
