/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/mock"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
)

// TestReadSetPreCheckNoConflict verifies the green-path: a block whose read-sets are all
// up-to-date produces a pre-check report with no conflicts, and the subsequent commit marks
// every transaction as VALID.
func TestReadSetPreCheckNoConflict(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	txid := util.GenerateUUID()
	simulator, err := kvLgr.NewTxSimulator(txid)
	require.NoError(t, err)
	require.NoError(t, simulator.SetState("ns1", "key1", []byte("value1")))
	simRes, err := simulator.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, err := simRes.GetPubSimulationBytes()
	require.NoError(t, err)
	simulator.Done()

	block := bg.NextBlockWithTxid([][]byte{pubSimBytes}, []string{txid})

	report := &ledger.ReadSetPreCheckReport{}
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block}, &ledger.CommitOptions{ReadSetPreCheck: report}))
	require.Equal(t, uint64(1), report.BlockNumber)
	require.Empty(t, report.ConflictTxIndices)
	require.Len(t, report.TxValidationCodes, 1)
	require.Equal(t, peer.TxValidationCode_VALID, report.TxValidationCodes[0])

	validCode, _, err := kvLgr.GetTxValidationCodeByTxID(txid)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_VALID, validCode)
}

// TestReadSetPreCheckStaleReadVersion verifies that a transaction whose read-set version is
// stale (because a concurrent update was committed after simulation) is reported as a conflict
// by the pre-check and marked as MVCC_READ_CONFLICT by the final commit.
func TestReadSetPreCheckStaleReadVersion(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	// Commit block 1: write key1=value1 (version 0)
	txid1 := util.GenerateUUID()
	sim1, err := kvLgr.NewTxSimulator(txid1)
	require.NoError(t, err)
	require.NoError(t, sim1.SetState("ns1", "key1", []byte("value1")))
	simRes1, err := sim1.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, err := simRes1.GetPubSimulationBytes()
	require.NoError(t, err)
	sim1.Done()
	block1 := bg.NextBlockWithTxid([][]byte{pubSimBytes1}, []string{txid1})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	// Simulate a transaction that reads key1 (version 0) and writes key1=value2.
	// This is simulated BEFORE the concurrent update lands, so the read version is stale.
	txid2 := util.GenerateUUID()
	sim2, err := kvLgr.NewTxSimulator(txid2)
	require.NoError(t, err)
	val, err := sim2.GetState("ns1", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	require.NoError(t, sim2.SetState("ns1", "key1", []byte("value2")))
	simRes2, err := sim2.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes2, err := simRes2.GetPubSimulationBytes()
	require.NoError(t, err)
	sim2.Done()

	// Concurrently commit an update to key1 (key1=value3) AFTER simulating but BEFORE committing
	// the simulated transaction. This makes the read version in txid2's read-set stale.
	txid3 := util.GenerateUUID()
	sim3, err := kvLgr.NewTxSimulator(txid3)
	require.NoError(t, err)
	require.NoError(t, sim3.SetState("ns1", "key1", []byte("value3")))
	simRes3, err := sim3.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes3, err := simRes3.GetPubSimulationBytes()
	require.NoError(t, err)
	sim3.Done()
	block2 := bg.NextBlockWithTxid([][]byte{pubSimBytes3}, []string{txid3})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block2}, &ledger.CommitOptions{}))

	// Now pre-check the stale transaction (block 3). The read version (0) is stale because
	// key1 was updated in block 2.
	block3 := bg.NextBlockWithTxid([][]byte{pubSimBytes2}, []string{txid2})
	report := &ledger.ReadSetPreCheckReport{}
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block3}, &ledger.CommitOptions{ReadSetPreCheck: report}))

	require.Equal(t, uint64(3), report.BlockNumber)
	require.Equal(t, []int{0}, report.ConflictTxIndices)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, report.TxValidationCodes[0])

	validCode, _, err := kvLgr.GetTxValidationCodeByTxID(txid2)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, validCode)

	// The pre-check must not have written to the ledger: key1 should still be value3
	qe, err := kvLgr.NewQueryExecutor()
	require.NoError(t, err)
	defer qe.Done()
	val, err = qe.GetState("ns1", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value3"), val)
}

// TestReadSetPreCheckWithinBlockConflict verifies that when two transactions in the same block
// touch the same key (the second reads a key that the first writes), the pre-check reports a
// conflict for the second transaction and the final commit marks it as MVCC_READ_CONFLICT.
func TestReadSetPreCheckWithinBlockConflict(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	txid1 := util.GenerateUUID()
	sim1, err := kvLgr.NewTxSimulator(txid1)
	require.NoError(t, err)
	require.NoError(t, sim1.SetState("ns1", "key1", []byte("value1")))
	simRes1, err := sim1.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, err := simRes1.GetPubSimulationBytes()
	require.NoError(t, err)
	sim1.Done()
	block1 := bg.NextBlockWithTxid([][]byte{pubSimBytes1}, []string{txid1})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	// Simulate tx A: reads key1 (version 0) and writes key1=valueA
	txidA := util.GenerateUUID()
	simA, err := kvLgr.NewTxSimulator(txidA)
	require.NoError(t, err)
	_, err = simA.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, simA.SetState("ns1", "key1", []byte("valueA")))
	simResA, err := simA.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytesA, err := simResA.GetPubSimulationBytes()
	require.NoError(t, err)
	simA.Done()

	// Simulate tx B: reads key1 (version 0) and writes key1=valueB.
	// Both tx A and tx B read key1 at version 0. When committed in the same block, tx A's write
	// makes tx B's read version stale.
	txidB := util.GenerateUUID()
	simB, err := kvLgr.NewTxSimulator(txidB)
	require.NoError(t, err)
	_, err = simB.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, simB.SetState("ns1", "key1", []byte("valueB")))
	simResB, err := simB.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytesB, err := simResB.GetPubSimulationBytes()
	require.NoError(t, err)
	simB.Done()

	block2 := bg.NextBlockWithTxid([][]byte{pubSimBytesA, pubSimBytesB}, []string{txidA, txidB})
	report := &ledger.ReadSetPreCheckReport{}
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block2}, &ledger.CommitOptions{ReadSetPreCheck: report}))

	require.Equal(t, uint64(2), report.BlockNumber)
	require.Equal(t, []int{1}, report.ConflictTxIndices)
	require.Equal(t, peer.TxValidationCode_VALID, report.TxValidationCodes[0])
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, report.TxValidationCodes[1])

	validCodeA, _, err := kvLgr.GetTxValidationCodeByTxID(txidA)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_VALID, validCodeA)

	validCodeB, _, err := kvLgr.GetTxValidationCodeByTxID(txidB)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, validCodeB)
}

// TestReadSetPreCheckDoesNotWrite verifies that the pre-check itself does not write anything to
// the ledger: the state before and after the pre-check is identical.
func TestReadSetPreCheckDoesNotWrite(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	txid1 := util.GenerateUUID()
	sim1, err := kvLgr.NewTxSimulator(txid1)
	require.NoError(t, err)
	require.NoError(t, sim1.SetState("ns1", "key1", []byte("value1")))
	simRes1, err := sim1.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, err := simRes1.GetPubSimulationBytes()
	require.NoError(t, err)
	sim1.Done()
	block1 := bg.NextBlockWithTxid([][]byte{pubSimBytes1}, []string{txid1})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	// Capture state before pre-check
	qeBefore, err := kvLgr.NewQueryExecutor()
	require.NoError(t, err)
	valBefore, err := qeBefore.GetState("ns1", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), valBefore)
	qeBefore.Done()

	// Simulate a transaction that reads key1 and writes key1=value2
	txid2 := util.GenerateUUID()
	sim2, err := kvLgr.NewTxSimulator(txid2)
	require.NoError(t, err)
	_, err = sim2.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, sim2.SetState("ns1", "key1", []byte("value2")))
	simRes2, err := sim2.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes2, err := simRes2.GetPubSimulationBytes()
	require.NoError(t, err)
	sim2.Done()

	// Run only the pre-check (do not commit). Construct the block directly (without
	// bg.NextBlock) so that the block number is not consumed by the pre-check.
	block2 := testutil.ConstructBlock(
		t, 2, protoutil.BlockHeaderHash(block1.GetHeader()),
		[][]byte{pubSimBytes2}, false,
	)
	report, err := kvLgr.preCheckReadSet(block2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), report.BlockNumber)
	require.Empty(t, report.ConflictTxIndices)

	// State after pre-check should be identical to state before pre-check
	qeAfter, err := kvLgr.NewQueryExecutor()
	require.NoError(t, err)
	defer qeAfter.Done()
	valAfter, err := qeAfter.GetState("ns1", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), valAfter)

	// Block height should still be 2 (genesis + block1); the pre-check did not commit block2
	bcInfo, err := kvLgr.GetBlockchainInfo()
	require.NoError(t, err)
	require.Equal(t, uint64(2), bcInfo.GetHeight())
}

// TestReadSetPreCheckTOCTOU verifies that even if the pre-check reports no conflict, the actual
// commit still re-validates the read-sets and marks a transaction as MVCC_READ_CONFLICT if a
// concurrent update landed between the pre-check and the commit (TOCTOU protection).
func TestReadSetPreCheckTOCTOU(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	txid1 := util.GenerateUUID()
	sim1, err := kvLgr.NewTxSimulator(txid1)
	require.NoError(t, err)
	require.NoError(t, sim1.SetState("ns1", "key1", []byte("value1")))
	simRes1, err := sim1.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, err := simRes1.GetPubSimulationBytes()
	require.NoError(t, err)
	sim1.Done()
	block1 := bg.NextBlockWithTxid([][]byte{pubSimBytes1}, []string{txid1})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	// Simulate a transaction that reads key1 (version 0) and writes key1=value2
	txid2 := util.GenerateUUID()
	sim2, err := kvLgr.NewTxSimulator(txid2)
	require.NoError(t, err)
	_, err = sim2.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, sim2.SetState("ns1", "key1", []byte("value2")))
	simRes2, err := sim2.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes2, err := simRes2.GetPubSimulationBytes()
	require.NoError(t, err)
	sim2.Done()

	// Run the pre-check: at this point key1 is still at version 0, so no conflict.
	// Construct the block directly (without bg.NextBlock) so that the block number is not
	// consumed by the pre-check.
	preCheckBlock := testutil.ConstructBlock(
		t, 2, protoutil.BlockHeaderHash(block1.GetHeader()),
		[][]byte{pubSimBytes2}, false,
	)
	report, err := kvLgr.preCheckReadSet(preCheckBlock)
	require.NoError(t, err)
	require.Empty(t, report.ConflictTxIndices)

	// Now simulate a concurrent update landing AFTER the pre-check but BEFORE the commit.
	// This is the TOCTOU scenario: the pre-check saw no conflict, but the state changed.
	txid3 := util.GenerateUUID()
	sim3, err := kvLgr.NewTxSimulator(txid3)
	require.NoError(t, err)
	require.NoError(t, sim3.SetState("ns1", "key1", []byte("value3")))
	simRes3, err := sim3.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes3, err := simRes3.GetPubSimulationBytes()
	require.NoError(t, err)
	sim3.Done()
	block2 := bg.NextBlockWithTxid([][]byte{pubSimBytes3}, []string{txid3})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block2}, &ledger.CommitOptions{}))

	// Now commit the original transaction (block 3). The pre-check said no conflict, but the
	// actual commit must re-validate and mark txid2 as MVCC_READ_CONFLICT.
	block3 := bg.NextBlockWithTxid([][]byte{pubSimBytes2}, []string{txid2})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block3}, &ledger.CommitOptions{}))

	validCode, _, err := kvLgr.GetTxValidationCodeByTxID(txid2)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, validCode)
}

// TestReadSetPreCheckMultipleTx verifies the pre-check correctly reports conflicts for a
// subset of transactions in a block.
func TestReadSetPreCheckMultipleTx(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	// Commit block 1: write key1=value1 and key2=value1 (version 0)
	txid1 := util.GenerateUUID()
	sim1, err := kvLgr.NewTxSimulator(txid1)
	require.NoError(t, err)
	require.NoError(t, sim1.SetState("ns1", "key1", []byte("value1")))
	require.NoError(t, sim1.SetState("ns1", "key2", []byte("value1")))
	simRes1, err := sim1.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes1, err := simRes1.GetPubSimulationBytes()
	require.NoError(t, err)
	sim1.Done()
	block1 := bg.NextBlockWithTxid([][]byte{pubSimBytes1}, []string{txid1})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block1}, &ledger.CommitOptions{}))

	// Simulate tx A: reads key1 (version 0) and writes key1=valueA
	txidA := util.GenerateUUID()
	simA, err := kvLgr.NewTxSimulator(txidA)
	require.NoError(t, err)
	_, err = simA.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, simA.SetState("ns1", "key1", []byte("valueA")))
	simResA, err := simA.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytesA, err := simResA.GetPubSimulationBytes()
	require.NoError(t, err)
	simA.Done()

	// Simulate tx B: reads key2 (version 0) and writes key2=valueB
	txidB := util.GenerateUUID()
	simB, err := kvLgr.NewTxSimulator(txidB)
	require.NoError(t, err)
	_, err = simB.GetState("ns1", "key2")
	require.NoError(t, err)
	require.NoError(t, simB.SetState("ns1", "key2", []byte("valueB")))
	simResB, err := simB.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytesB, err := simResB.GetPubSimulationBytes()
	require.NoError(t, err)
	simB.Done()

	// Simulate tx C: reads key1 (version 0) and writes key1=valueC
	txidC := util.GenerateUUID()
	simC, err := kvLgr.NewTxSimulator(txidC)
	require.NoError(t, err)
	_, err = simC.GetState("ns1", "key1")
	require.NoError(t, err)
	require.NoError(t, simC.SetState("ns1", "key1", []byte("valueC")))
	simResC, err := simC.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytesC, err := simResC.GetPubSimulationBytes()
	require.NoError(t, err)
	simC.Done()

	// Commit block 2: update key1=value2 (concurrent update). This makes key1's read version
	// stale for tx A and tx C, which read key1 at version 0.
	txid2 := util.GenerateUUID()
	sim2, err := kvLgr.NewTxSimulator(txid2)
	require.NoError(t, err)
	require.NoError(t, sim2.SetState("ns1", "key1", []byte("value2")))
	simRes2, err := sim2.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes2, err := simRes2.GetPubSimulationBytes()
	require.NoError(t, err)
	sim2.Done()
	block2 := bg.NextBlockWithTxid([][]byte{pubSimBytes2}, []string{txid2})
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block2}, &ledger.CommitOptions{}))

	// Commit all three simulated transactions in block 3
	block3 := bg.NextBlockWithTxid(
		[][]byte{pubSimBytesA, pubSimBytesB, pubSimBytesC},
		[]string{txidA, txidB, txidC},
	)
	report := &ledger.ReadSetPreCheckReport{}
	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block3}, &ledger.CommitOptions{ReadSetPreCheck: report}))

	// Pre-check should report conflicts for tx A (index 0) and tx C (index 2), but not tx B (index 1)
	require.Equal(t, uint64(3), report.BlockNumber)
	require.ElementsMatch(t, []int{0, 2}, report.ConflictTxIndices)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, report.TxValidationCodes[0])
	require.Equal(t, peer.TxValidationCode_VALID, report.TxValidationCodes[1])
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, report.TxValidationCodes[2])

	validCodeA, _, err := kvLgr.GetTxValidationCodeByTxID(txidA)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, validCodeA)

	validCodeB, _, err := kvLgr.GetTxValidationCodeByTxID(txidB)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_VALID, validCodeB)

	validCodeC, _, err := kvLgr.GetTxValidationCodeByTxID(txidC)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, validCodeC)
}

// TestReadSetPreCheckNilOption verifies that when the ReadSetPreCheck option is nil, the commit
// behaves exactly as before (no pre-check is performed).
func TestReadSetPreCheckNilOption(t *testing.T) {
	conf := testConfig(t)
	provider := testutilNewProvider(conf, t, &mock.DeployedChaincodeInfoProvider{})
	defer provider.Close()

	bg, gb := testutil.NewBlockGenerator(t, "testLedger", false)
	lgr, err := provider.CreateFromGenesisBlock(gb)
	require.NoError(t, err)
	defer lgr.Close()

	kvLgr := lgr.(*kvLedger)

	txid := util.GenerateUUID()
	sim, err := kvLgr.NewTxSimulator(txid)
	require.NoError(t, err)
	require.NoError(t, sim.SetState("ns1", "key1", []byte("value1")))
	simRes, err := sim.GetTxSimulationResults()
	require.NoError(t, err)
	pubSimBytes, err := simRes.GetPubSimulationBytes()
	require.NoError(t, err)
	sim.Done()

	block := bg.NextBlockWithTxid([][]byte{pubSimBytes}, []string{txid})

	require.NoError(t, kvLgr.CommitLegacy(&ledger.BlockAndPvtData{Block: block}, &ledger.CommitOptions{}))

	validCode, _, err := kvLgr.GetTxValidationCodeByTxID(txid)
	require.NoError(t, err)
	require.Equal(t, peer.TxValidationCode_VALID, validCode)
}
