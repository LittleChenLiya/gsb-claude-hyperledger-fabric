/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	fablibdisabled "github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	cbpeer "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/snapshot"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/memdb"
	"github.com/hyperledger/fabric/core/ledger/mock"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
)

// TestPrecheckReadSetConflictsInMemory exercises the pre-block-commit read-set precheck
// against a fully in-memory ledger (no leveldb/couchdb/filesystem). It covers:
//  1. a stale read against an already committed newer version,
//  2. a conflict caused by a preceding write within the same candidate block,
//  3. a clean candidate block,
//  4. the TOCTOU window: a precheck that passes but a concurrent block commit that
//     makes the final commit-time validation invalidate the transaction.
//
// In every case, the precheck itself must leave the ledger untouched and the actual
// commit must re-run the MVCC validation.
func TestPrecheckReadSetConflictsInMemory(t *testing.T) {
	env := newInMemoryLedgerEnv(t)

	// kvLedger must expose both optional entry points through the public interfaces.
	var _ ledger.ReadSetPrechecker = env.kvLgr
	var _ ledger.CommitWithPrechecker = env.kvLgr

	// block 1: write ns1/k=v1 -> committed version (1,0)
	b1Sim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
		b.AddToWriteSet("ns1", "k", []byte("v1"))
	})
	block1 := env.bg.NextBlockWithTxid([][]byte{b1Sim}, []string{"tx-write-k-v1"})
	require.NoError(t, env.lgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	committedVersion, err := env.stateDB.GetVersion("ns1", "k")
	require.NoError(t, err)
	require.Equal(t, version.NewHeight(1, 0), committedVersion)

	t.Run("stale_read_against_committed_version", func(t *testing.T) {
		heightBefore := env.blockchainHeight(t)

		// The transaction read ns1/k at the genesis state (version nil), which is stale
		// as the key is now committed at (1,0).
		staleSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToReadSet("ns1", "k", nil)
			b.AddToWriteSet("ns1", "k2", []byte("stale"))
		})
		candidate := env.bg.NextBlockWithTxid([][]byte{staleSim}, []string{"tx-stale-read"})

		// The candidate block must not be present in the ledger before the precheck.
		_, err = env.blockStore.RetrieveBlockByNumber(candidate.GetHeader().GetNumber())
		require.Error(t, err)

		report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(&ledger.BlockAndPvtData{Block: candidate})
		require.NoError(t, err)
		require.True(t, report.HasConflicts())
		require.Len(t, report.TxResults, 1)
		result := report.TxResults[0]
		require.Equal(t, uint64(0), result.TxIndexInBlock)
		require.Equal(t, "tx-stale-read", result.TxID)
		require.NotNil(t, result.Conflict)
		require.Equal(t, ledger.MVCCReadConflict, result.Conflict.Type)
		require.Equal(t, "ns1", result.Conflict.Namespace)
		require.Equal(t, "k", result.Conflict.Key)
		require.Equal(t, uint64(0), result.Conflict.ReadBlockNum)
		require.Equal(t, uint64(0), result.Conflict.ReadTxNum)
		require.Equal(t, uint64(1), result.Conflict.CommittedBlockNum)
		require.Equal(t, uint64(0), result.Conflict.CommittedTxNum)

		// Precheck is strictly read-only: height and committed state are unchanged, the
		// candidate block is not stored, and its filter metadata is untouched.
		require.Equal(t, heightBefore, env.blockchainHeight(t))
		_, err = env.blockStore.RetrieveBlockByNumber(candidate.GetHeader().GetNumber())
		require.Error(t, err)
		v, err := env.stateDB.GetState("ns1", "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), v.Value)
		require.Equal(t, version.NewHeight(1, 0), v.Version)
		require.Len(t, candidate.GetMetadata().GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER], 1)
		require.Equal(t,
			cbpeer.TxValidationCode_VALID,
			cbpeer.TxValidationCode(candidate.GetMetadata().GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER][0]),
		)

		// Final commit: commit succeeds as an operation but the transaction is marked
		// MVCC_READ_CONFLICT after re-validation and its write does not land in state.
		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
		))
		code, blkNum, err := env.lgr.GetTxValidationCodeByTxID("tx-stale-read")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_MVCC_READ_CONFLICT, code)
		require.Equal(t, candidate.GetHeader().GetNumber(), blkNum)
		v, err = env.stateDB.GetState("ns1", "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), v.Value)
		missing, err := env.stateDB.GetState("ns1", "k2")
		require.NoError(t, err)
		require.Nil(t, missing)
	})

	t.Run("conflict_with_preceding_tx_in_same_block", func(t *testing.T) {
		// tx0 writes ns1/k (fresh enough for its own read-set-less tx), tx1 reads ns1/k
		// at (1,0); tx0's in-block write invalidates tx1.
		writeSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToWriteSet("ns1", "k", []byte("v2"))
		})
		readSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToReadSet("ns1", "k", version.NewHeight(1, 0))
			b.AddToWriteSet("ns1", "k3", []byte("in-block"))
		})
		candidate := env.bg.NextBlockWithTxid(
			[][]byte{writeSim, readSim},
			[]string{"tx-write-k-v2", "tx-read-k-after-write"},
		)

		report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(
			&ledger.BlockAndPvtData{Block: candidate},
		)
		require.NoError(t, err)
		require.True(t, report.HasConflicts())
		require.Len(t, report.TxResults, 2)
		require.Nil(t, report.TxResults[0].Conflict)
		require.NotNil(t, report.TxResults[1].Conflict)
		require.Equal(t, ledger.MVCCReadConflict, report.TxResults[1].Conflict.Type)
		require.Equal(t, "k", report.TxResults[1].Conflict.Key)

		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
		))
		code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-read-k-after-write")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_MVCC_READ_CONFLICT, code)
		code0, _, err := env.lgr.GetTxValidationCodeByTxID("tx-write-k-v2")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_VALID, code0)
		// the valid preceding tx landed its write
		v, err := env.stateDB.GetState("ns1", "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), v.Value)
		require.Equal(t, version.NewHeight(3, 0), v.Version)
	})

	t.Run("clean_read_set_passes_precheck_and_commit", func(t *testing.T) {
		cleanSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToReadSet("ns1", "k", version.NewHeight(3, 0))
			b.AddToWriteSet("ns1", "clean", []byte("ok"))
		})
		candidate := env.bg.NextBlockWithTxid([][]byte{cleanSim}, []string{"tx-clean"})

		report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(
			&ledger.BlockAndPvtData{Block: candidate},
		)
		require.NoError(t, err)
		require.False(t, report.HasConflicts())
		require.Nil(t, report.TxResults[0].Conflict)

		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
		))
		code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-clean")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_VALID, code)
		v, err := env.stateDB.GetState("ns1", "clean")
		require.NoError(t, err)
		require.Equal(t, []byte("ok"), v.Value)
	})

	t.Run("toctou_precheck_passes_but_concurrent_commit_invalidates", func(t *testing.T) {
		// A transaction reads ns1/k at the current version (3,0). Its precheck passes.
		// Before its own block is committed, a concurrent block commits a newer write of
		// ns1/k. The final commit-time MVCC validation must still reject the transaction.
		racerBuilder := func() []byte {
			return newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
				b.AddToReadSet("ns1", "k", version.NewHeight(3, 0))
				b.AddToWriteSet("ns1", "racer", []byte("maybe"))
			})
		}
		// Pre-check the candidate (constructed at the current next-block height without
		// advancing the block generator) while the committed version is still (3,0):
		// no conflict.
		info, err := env.lgr.GetBlockchainInfo()
		require.NoError(t, err)
		precheckCandidate := testutil.ConstructBlockWithTxid(
			t, info.GetHeight(), info.GetCurrentBlockHash(),
			[][]byte{racerBuilder()}, []string{"tx-racer-precheck"}, false,
		)
		report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(
			&ledger.BlockAndPvtData{Block: precheckCandidate},
		)
		require.NoError(t, err)
		require.False(t, report.HasConflicts())

		// "Concurrent" block: another orderer-delivered block arrives while the candidate
		// is still uncommitted and moves ns1/k to (5,0).
		concurrentSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToWriteSet("ns1", "k", []byte("concurrent"))
		})
		concurrentBlock := env.bg.NextBlockWithTxid([][]byte{concurrentSim}, []string{"tx-concurrent"})
		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: concurrentBlock}, &ledger.CommitOptions{},
		))

		// Now assemble the candidate at the next block height with the same stale read-set.
		candidate := env.bg.NextBlockWithTxid([][]byte{racerBuilder()}, []string{"tx-racer"})

		// re-running the precheck now reports the conflict that was invisible earlier
		report2, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(
			&ledger.BlockAndPvtData{Block: candidate},
		)
		require.NoError(t, err)
		require.True(t, report2.HasConflicts())
		require.Equal(t, ledger.MVCCReadConflict, report2.TxResults[0].Conflict.Type)

		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
		))
		code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-racer")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_MVCC_READ_CONFLICT, code)
	})

	t.Run("toctou_precheck_runs_while_commit_changes_state", func(t *testing.T) {
		// Block commits are serialized by the ledger (oldBlockCommit), so the race this
		// exercises is between a read-only precheck and the state write of a commit. The
		// precheck takes commitRWLock.RLock and therefore must never block, or be blocked
		// by, an in-flight query executor; a commit still re-validates everything.
		//
		// Here, a racer tx observes (6,0) and prechecks clean. A concurrent committed
		// block advances ns1/k; when the racer block is subsequently committed, its tx is
		// invalidated regardless of the earlier clean precheck.
		info, err := env.lgr.GetBlockchainInfo()
		require.NoError(t, err)
		committedAt, err := env.stateDB.GetVersion("ns1", "k")
		require.NoError(t, err)

		racerSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToReadSet("ns1", "k", committedAt)
			b.AddToWriteSet("ns1", "racer2", []byte("maybe"))
		})
		precheckBlock := testutil.ConstructBlockWithTxid(
			t, info.GetHeight(), info.GetCurrentBlockHash(),
			[][]byte{racerSim}, []string{"tx-racer2-precheck"}, false,
		)
		report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(&ledger.BlockAndPvtData{Block: precheckBlock})
		require.NoError(t, err)
		require.False(t, report.HasConflicts())

		// Many read-only prechecks can proceed in parallel: they never take a write lock.
		const parallelPrechecks = 16
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range parallelPrechecks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, _ = env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(&ledger.BlockAndPvtData{Block: precheckBlock})
			}()
		}
		close(start)
		wg.Wait()

		// Concurrent committer advances the key after the clean precheck.
		updaterSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
			b.AddToWriteSet("ns1", "k", []byte("after-precheck"))
		})
		updaterBlock := env.bg.NextBlockWithTxid([][]byte{updaterSim}, []string{"tx-after-precheck"})
		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: updaterBlock}, &ledger.CommitOptions{},
		))

		candidate := env.bg.NextBlockWithTxid([][]byte{racerSim}, []string{"tx-racer2"})
		// precheck now must detect the TOCTOU conflict
		report, err = env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(&ledger.BlockAndPvtData{Block: candidate})
		require.NoError(t, err)
		require.True(t, report.HasConflicts())
		require.Equal(t, ledger.MVCCReadConflict, report.TxResults[0].Conflict.Type)

		require.NoError(t, env.lgr.CommitLegacy(
			&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
		))
		code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-racer2")
		require.NoError(t, err)
		require.Equal(t, cbpeer.TxValidationCode_MVCC_READ_CONFLICT, code)
	})
}

// TestPrecheckPhantomReadConflictInMemory verifies that a range query whose results have
// changed since simulation is reported as a PhantomReadConflict by the in-memory ledger.
func TestPrecheckPhantomReadConflictInMemory(t *testing.T) {
	env := newInMemoryLedgerEnv(t)

	// commit a key inside the range that the phantom tx will query
	writeSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
		b.AddToWriteSet("ns1", "phantom-key", []byte("v"))
	})
	require.NoError(t, env.lgr.CommitLegacy(
		&ledger.BlockAndPvtData{Block: env.bg.NextBlock([][]byte{writeSim})},
		&ledger.CommitOptions{},
	))

	// The tx simulated a range [phantom-, phantoz) over an empty state (no raw reads and
	// the iterator was exhausted). After the commit above, the range contains one key.
	phantomSim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
		rqi := &kvrwset.RangeQueryInfo{StartKey: "phantom-", EndKey: "phantoz", ItrExhausted: true}
		rwsetutil.SetRawReads(rqi, nil)
		b.AddToRangeQuerySet("ns1", rqi)
	})
	candidate := env.bg.NextBlockWithTxid([][]byte{phantomSim}, []string{"tx-phantom"})

	report, err := env.lgr.(ledger.ReadSetPrechecker).PrecheckReadSetConflicts(
		&ledger.BlockAndPvtData{Block: candidate},
	)
	require.NoError(t, err)
	require.True(t, report.HasConflicts())
	require.NotNil(t, report.TxResults[0].Conflict)
	conflict := report.TxResults[0].Conflict
	require.Equal(t, ledger.PhantomReadConflict, conflict.Type)
	require.Equal(t, "ns1", conflict.Namespace)
	require.Equal(t, "phantom-", conflict.StartKey)
	require.Equal(t, "phantoz", conflict.EndKey)

	// commit still runs the full validation and marks the tx invalid with PHANTOM_READ_CONFLICT
	require.NoError(t, env.lgr.CommitLegacy(
		&ledger.BlockAndPvtData{Block: candidate}, &ledger.CommitOptions{},
	))
	code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-phantom")
	require.NoError(t, err)
	require.Equal(t, cbpeer.TxValidationCode_PHANTOM_READ_CONFLICT, code)
}

// TestCommitLegacyWithPrecheck verifies the combined entry point returns the advisory
// report together with a successful commit for a clean block.
func TestCommitLegacyWithPrecheck(t *testing.T) {
	env := newInMemoryLedgerEnv(t)

	sim := newSimResults(t, func(b *rwsetutil.RWSetBuilder) {
		b.AddToWriteSet("ns1", "p", []byte("q"))
	})
	candidate := env.bg.NextBlockWithTxid([][]byte{sim}, []string{"tx-with-precheck"})

	result, err := env.lgr.(ledger.CommitWithPrechecker).CommitLegacyWithPrecheck(
		&ledger.BlockAndPvtData{Block: candidate},
		&ledger.CommitOptions{PrecheckReadSet: true},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.PrecheckReport)
	require.False(t, result.PrecheckReport.HasConflicts())

	code, _, err := env.lgr.GetTxValidationCodeByTxID("tx-with-precheck")
	require.NoError(t, err)
	require.Equal(t, cbpeer.TxValidationCode_VALID, code)
}

// ----- in-memory ledger test harness -----

type inMemoryLedgerEnv struct {
	t          *testing.T
	lgr        ledger.PeerLedger
	kvLgr      *kvLedger
	blockStore *memBlockStore
	pvtStore   *memPvtDataStore
	stateDB    *memdb.DB
	bg         *testutil.BlockGenerator
}

func (env *inMemoryLedgerEnv) blockchainHeight(t *testing.T) uint64 {
	info, err := env.lgr.GetBlockchainInfo()
	require.NoError(t, err)
	return info.GetHeight()
}

func newInMemoryLedgerEnv(t *testing.T) *inMemoryLedgerEnv {
	const ledgerID = "testprecheckreadset"

	bg, gb := testutil.NewBlockGenerator(t, ledgerID, false)

	memProvider := memdb.NewProvider()
	vdb, err := memProvider.GetDBHandle(ledgerID, nil)
	require.NoError(t, err)
	memStateDB := vdb.(*memdb.DB)

	stateDB, err := privacyenabledstate.NewDB(vdb, ledgerID, nil)
	require.NoError(t, err)

	blockStore := newMemBlockStore(gb)
	pvtStore := newMemPvtDataStore()

	cryptoProvider := factory.GetDefault()
	stats := newStats(&fablibdisabled.Provider{}).ledgerStats(ledgerID)

	// A config-history manager backed by no leveldb: the retriever itself is never
	// invoked by these tests, but newKVLedger needs a non-nil value to wrap.
	configRetriever := &collectionConfigHistoryRetriever{
		Retriever:                     &confighistory.Retriever{},
		DeployedChaincodeInfoProvider: &mock.DeployedChaincodeInfoProvider{},
	}

	l, err := newKVLedger(&lgrInitializer{
		ledgerID:                 ledgerID,
		blockStore:               blockStore,
		pvtdataStore:             pvtStore,
		stateDB:                  stateDB,
		ccInfoProvider:           &mock.DeployedChaincodeInfoProvider{},
		ccLifecycleEventProvider: &mock.ChaincodeLifecycleEventProvider{},
		stats:                    stats,
		hashProvider:             cryptoProvider,
		config: &ledger.Config{
			StateDBConfig:     &ledger.StateDBConfig{},
			HistoryDBConfig:   &ledger.HistoryDBConfig{Enabled: false},
			SnapshotsConfig:   &ledger.SnapshotsConfig{},
			PrivateDataConfig: &ledger.PrivateDataConfig{MaxBatchSize: 5000, BatchesInterval: 1000, PurgeInterval: 100},
		},
		configHistoryRetriever: configRetriever,
		snapshotMgr:            &noopCommitSync{},
	})
	require.NoError(t, err)
	t.Cleanup(l.Close)

	env := &inMemoryLedgerEnv{
		t:          t,
		lgr:        l,
		kvLgr:      l,
		blockStore: blockStore,
		pvtStore:   pvtStore,
		stateDB:    memStateDB,
		bg:         bg,
	}
	return env
}

func newSimResults(t *testing.T, build func(*rwsetutil.RWSetBuilder)) []byte {
	b := rwsetutil.NewRWSetBuilder()
	build(b)
	simRes, err := b.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, err := simRes.GetPubSimulationBytes()
	require.NoError(t, err)
	return pubSimBytes
}

// ----- no-op snapshot synchronizer -----

type noopCommitSync struct{}

func (*noopCommitSync) commitStart(uint64)                    {}
func (*noopCommitSync) commitDone(uint64)                     {}
func (*noopCommitSync) submitRequest(eventType, uint64) error { return nil }
func (*noopCommitSync) pendingRequests() ([]uint64, error)    { return nil, nil }
func (*noopCommitSync) shutdown()                             {}

// ----- in-memory block store -----

type memBlockStore struct {
	mu      sync.RWMutex
	blocks  map[uint64]*common.Block
	txIndex map[string]txLocation
}

type txLocation struct {
	blockNum uint64
	txNum    uint64
}

func newMemBlockStore(blocks ...*common.Block) *memBlockStore {
	s := &memBlockStore{
		blocks:  map[uint64]*common.Block{},
		txIndex: map[string]txLocation{},
	}
	for _, b := range blocks {
		if err := s.AddBlock(b); err != nil {
			panic(err)
		}
	}
	return s
}

func (s *memBlockStore) AddBlock(block *common.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	blockNum := block.GetHeader().GetNumber()
	s.blocks[blockNum] = block
	for txNum, envBytes := range block.GetData().GetData() {
		if len(envBytes) == 0 {
			continue
		}
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return err
		}
		payload, err := protoutil.UnmarshalPayload(env.GetPayload())
		if err != nil {
			return err
		}
		chdr, err := protoutil.UnmarshalChannelHeader(payload.GetHeader().GetChannelHeader())
		if err != nil {
			return err
		}
		s.txIndex[chdr.GetTxId()] = txLocation{blockNum: blockNum, txNum: uint64(txNum)}
	}
	return nil
}

func (s *memBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	height := uint64(len(s.blocks))
	info := &common.BlockchainInfo{Height: height}
	if b, ok := s.blocks[height-1]; ok {
		info.CurrentBlockHash = protoutil.BlockHeaderHash(b.GetHeader())
		info.PreviousBlockHash = b.GetHeader().GetPreviousHash()
	}
	return info, nil
}

func (s *memBlockStore) RetrieveBlocks(startNum uint64) (commonledger.ResultsIterator, error) {
	return &memBlocksItr{store: s, next: startNum}, nil
}

func (s *memBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.blocks {
		if string(protoutil.BlockHeaderHash(b.GetHeader())) == string(blockHash) {
			return b, nil
		}
	}
	return nil, errors.Errorf("block with hash [%x] not found in in-memory block store", blockHash)
}

func (s *memBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[blockNum]
	if !ok {
		return nil, errors.Errorf("block [%d] not found in in-memory block store", blockNum)
	}
	return b, nil
}

func (s *memBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	loc, ok := s.loc(txID)
	if !ok {
		return nil, errors.Errorf("txID [%s] not found in in-memory block store", txID)
	}
	return s.RetrieveBlockByNumber(loc.blockNum)
}

func (s *memBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	loc, ok := s.loc(txID)
	if !ok {
		return nil, errors.Errorf("txID [%s] not found in in-memory block store", txID)
	}
	block, err := s.RetrieveBlockByNumber(loc.blockNum)
	if err != nil {
		return nil, err
	}
	return protoutil.GetEnvelopeFromBlock(block.GetData().GetData()[loc.txNum])
}

func (s *memBlockStore) RetrieveTxByBlockNumTranNum(blockNum, txNum uint64) (*common.Envelope, error) {
	block, err := s.RetrieveBlockByNumber(blockNum)
	if err != nil {
		return nil, err
	}
	return protoutil.GetEnvelopeFromBlock(block.GetData().GetData()[txNum])
}

func (s *memBlockStore) RetrieveTxValidationCodeByTxID(txID string) (cbpeer.TxValidationCode, uint64, error) {
	loc, ok := s.loc(txID)
	if !ok {
		return cbpeer.TxValidationCode(0), 0, errors.Errorf("txID [%s] not found in in-memory block store", txID)
	}
	block, err := s.RetrieveBlockByNumber(loc.blockNum)
	if err != nil {
		return cbpeer.TxValidationCode(0), 0, err
	}
	filter := txflags.ValidationFlags(block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	return filter.Flag(int(loc.txNum)), loc.blockNum, nil
}

func (s *memBlockStore) TxIDExists(txID string) (bool, error) {
	_, ok := s.loc(txID)
	return ok, nil
}

func (s *memBlockStore) ExportTxIds(string, snapshot.NewHashFunc) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}

func (s *memBlockStore) Shutdown() {}

func (s *memBlockStore) loc(txID string) (txLocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	loc, ok := s.txIndex[txID]
	return loc, ok
}

type memBlocksItr struct {
	store *memBlockStore
	next  uint64
}

func (itr *memBlocksItr) Next() (commonledger.QueryResult, error) {
	block, err := itr.store.RetrieveBlockByNumber(itr.next)
	if err != nil {
		return nil, err
	}
	itr.next++
	return block, nil
}

func (itr *memBlocksItr) Close() {}

// ----- in-memory private data store -----

type memPvtDataStore struct {
	mu     sync.Mutex
	height uint64
}

func newMemPvtDataStore() *memPvtDataStore {
	return &memPvtDataStore{}
}

func (s *memPvtDataStore) Init(pvtdatapolicy.BTLPolicy) {}

func (s *memPvtDataStore) LastCommittedBlockHeight() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.height, nil
}

func (s *memPvtDataStore) Commit(blockNum uint64, _ []*ledger.TxPvtData, _ ledger.TxMissingPvtData, _ []*pvtdatastorage.PurgeMarker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.height = blockNum + 1
	return nil
}

func (s *memPvtDataStore) GetPvtDataByBlockNum(uint64, ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	return nil, nil
}

func (s *memPvtDataStore) GetLastUpdatedOldBlocksPvtData() (map[uint64][]*ledger.TxPvtData, error) {
	return nil, nil
}

func (s *memPvtDataStore) ResetLastUpdatedOldBlocksList() error { return nil }

func (s *memPvtDataStore) FetchPrivateDataRawKey(string, string, []byte) (string, error) {
	return "", nil
}

func (s *memPvtDataStore) CommitPvtDataOfOldBlocks(map[uint64][]*ledger.TxPvtData, ledger.MissingPvtDataInfo) error {
	return nil
}

func (s *memPvtDataStore) GetMissingPvtDataInfoForMostRecentBlocks(uint64, int) (ledger.MissingPvtDataInfo, error) {
	return nil, nil
}

func (s *memPvtDataStore) FetchBootKVHashes(uint64, uint64, string, string) (map[string][]byte, error) {
	return nil, nil
}

func (s *memPvtDataStore) RemoveAppInitiatedPurgesUsingReconMarker(
	kvHashes map[string][]byte, _, _ string, _, _ uint64,
) (map[string][]byte, error) {
	return kvHashes, nil
}
