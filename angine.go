// Copyright 2017 ZhongAn Information Technology Services Co.,Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package angine

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/annchain/angine/blockchain"
	"github.com/annchain/angine/blockchain/archive"
	"github.com/annchain/angine/blockchain/refuse_list"
	ac "github.com/annchain/angine/config"
	"github.com/annchain/angine/consensus"
	"github.com/annchain/angine/mempool"
	"github.com/annchain/angine/plugin"
	"github.com/annchain/angine/state"
	"github.com/annchain/angine/trace"
	"github.com/annchain/angine/types"
	"github.com/annchain/angine/utils/zip"
	"github.com/annchain/ann-module/lib/ed25519"
	cmn "github.com/annchain/ann-module/lib/go-common"
	crypto "github.com/annchain/ann-module/lib/go-crypto"
	dbm "github.com/annchain/ann-module/lib/go-db"
	"github.com/annchain/ann-module/lib/go-events"
	p2p "github.com/annchain/ann-module/lib/go-p2p"
	"github.com/annchain/ann-module/lib/go-wire"
	"github.com/annchain/go-sdk/ti"
)

const version = "0.6.0"

type (
	// Angine is a high level abstraction of all the state, consensus, mempool blah blah...
	Angine struct {
		Tune *Tunes

		mtx     sync.Mutex
		tune    *Tunes
		hooked  bool
		started bool

		statedb       dbm.DB
		blockdb       dbm.DB
		archivedb     dbm.DB
		querydb       dbm.DB
		privValidator *types.PrivValidator
		blockstore    *blockchain.BlockStore
		dataArchive   *archive.Archive
		conf          *viper.Viper
		mempool       *mempool.Mempool
		consensus     *consensus.ConsensusState
		traceRouter   *trace.Router
		stateMachine  *state.State
		p2pSwitch     *p2p.Switch
		eventSwitch   *types.EventSwitch
		refuseList    *refuse_list.RefuseList
		p2pHost       string
		p2pPort       uint16
		genesis       *types.GenesisDoc
		addrBook      *p2p.AddrBook

		logger *zap.Logger

		getSpecialVote func([]byte, *types.Validator) ([]byte, error)
	}

	// Tunes wraps two different kinds of configurations for angine
	Tunes struct {
		Runtime string
		Conf    *viper.Viper
	}
)

// ProtocolAndAddress accepts tcp by default
func ProtocolAndAddress(listenAddr string) (string, string) {
	protocol, address := "tcp", listenAddr
	parts := strings.SplitN(address, "://", 2)
	if len(parts) == 2 {
		protocol, address = parts[0], parts[1]
	}
	return protocol, address
}

// Initialize generates genesis.json and priv_validator.json automatically.
// It is usually used with commands like "init" before user put the node into running.
func Initialize(tune *Tunes, chainID string) {
	var conf *viper.Viper
	if tune.Conf == nil {
		conf = ac.GetConfig(tune.Runtime)
	} else {
		conf = tune.Conf
	}
	conf.AutomaticEnv()

	priv := genPrivFile(conf.GetString("priv_validator_file"))
	gvs := []types.GenesisValidator{types.GenesisValidator{
		PubKey: priv.PubKey,
		Amount: 100,
		IsCA:   true,
	}}
	genDoc, err := genGenesiFile(conf.GetString("genesis_file"), chainID, gvs)
	if err != nil {
		cmn.PanicSanity(err)
	}

	fmt.Println("Initialized ", genDoc.ChainID, "genesis", conf.GetString("genesis_file"), "priv_validator", conf.GetString("priv_validator_file"))
	fmt.Println("Check the files generated, make sure everything is OK.")
}

// NewAngine makes and returns a new angine, which can be used directly after being imported
func NewAngine(lgr *zap.Logger, tune *Tunes) *Angine {
	var conf *viper.Viper
	if tune.Conf == nil {
		conf = ac.GetConfig(tune.Runtime)
	} else {
		conf = tune.Conf
	}
	conf.AutomaticEnv()

	dbBackend := conf.GetString("db_backend")
	dbDir := conf.GetString("db_dir")
	dbArchiveDir := conf.GetString("db_archive_dir")
	stateDB := dbm.NewDB("state", dbBackend, dbDir)
	blockStoreDB := dbm.NewDB("blockstore", dbBackend, dbDir)
	archiveDB := dbm.NewDB("blockstore", dbBackend, dbArchiveDir)
	querydb, err := ensureQueryDB(dbDir)
	if err != nil {
		// querydb failure is something that we can bear with
		lgr.Error("angine error", zap.Error(err))
		fmt.Println(err)
	}
	gotGenesis := true
	genesis, err := getGenesisFile(conf) // ignore any error
	if err != nil {
		gotGenesis = false
	}
	if gotGenesis {
		conf.Set("chain_id", genesis.ChainID)
	}
	chainID := conf.GetString("chain_id")
	logger, err := getLogger(conf, chainID)
	if err != nil {
		lgr.Error("fail to get logger", zap.Error(err))
		return nil
	}

	stateM, err := getOrMakeState(logger, conf, stateDB, genesis)
	if err != nil {
		lgr.Error("angine error", zap.Error(err))
		return nil
	}

	privValidator := types.LoadOrGenPrivValidator(logger, conf.GetString("priv_validator_file"))
	refuseList := refuse_list.NewRefuseList(dbBackend, dbDir)
	eventSwitch := types.NewEventSwitch(logger)
	if _, err := eventSwitch.Start(); err != nil {
		lgr.Error("fail to start event switch", zap.Error(err))
		return nil
	}

	gb := make([]byte, 0)
	if gotGenesis {
		gb = wire.JSONBytes(genesis)
	}
	p2psw, err := prepareP2P(logger, conf, gb, privValidator, refuseList)
	if err != nil {
		lgr.Error("prepare p2p err", zap.Error(err))
		return nil
	}
	p2pListener := p2psw.Listeners()[0]

	if tune.Conf == nil {
		tune.Conf = conf
	} else if tune.Runtime == "" {
		tune.Runtime = conf.GetString("runtime")
	}
	dataArchive := archive.NewArchive(dbBackend, dbDir, conf.GetInt("threshold_blocks"))
	angine := &Angine{
		Tune: tune,

		statedb:     stateDB,
		blockdb:     blockStoreDB,
		archivedb:   archiveDB,
		querydb:     querydb,
		tune:        tune,
		dataArchive: dataArchive,
		conf:        conf,

		p2pSwitch:     p2psw,
		eventSwitch:   &eventSwitch,
		refuseList:    refuseList,
		privValidator: privValidator,
		p2pHost:       p2pListener.ExternalAddress().IP.String(),
		p2pPort:       p2pListener.ExternalAddress().Port,
		genesis:       genesis,

		logger: logger,
	}

	if gotGenesis {
		angine.assembleStateMachine(stateM)
	} else {
		p2psw.SetGenesisUnmarshal(func(b []byte) (err error) {
			defer func() {
				if e := recover(); e != nil {
					err = errors.Errorf("%v", e)
				}
			}()

			g := types.GenesisDocFromJSON(b)
			if g.ChainID != chainID {
				return fmt.Errorf("wrong chain id from genesis, expect %v, got %v", chainID, g.ChainID)
			}
			if err := g.SaveAs(conf.GetString("genesis_file")); err != nil {
				return err
			}
			angine.genesis = g
			angine.assembleStateMachine(state.MakeGenesisState(logger, stateDB, g))
			// here we defer the Start of reactors when we really have them
			for _, r := range angine.p2pSwitch.Reactors() {
				if _, err := r.Start(); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return angine
}

func (ang *Angine) assembleStateMachine(stateM *state.State) {
	conf := ang.tune.Conf

	fastSync := fastSyncable(conf, ang.privValidator.GetAddress(), stateM.Validators)
	stateM.SetLogger(ang.logger)
	stateM.SetQueryDB(ang.querydb)

	blockStore := blockchain.NewBlockStore(ang.blockdb, ang.archivedb)
	_, stateLastHeight, _ := stateM.GetLastBlockInfo()
	bcReactor := blockchain.NewBlockchainReactor(ang.logger, conf, stateLastHeight, blockStore, fastSync, ang.dataArchive)
	mem := mempool.NewMempool(ang.logger, conf)
	for _, p := range stateM.Plugins {
		mem.RegisterFilter(NewMempoolFilter(p.CheckTx))
	}
	memReactor := mempool.NewMempoolReactor(ang.logger, conf, mem)

	consensusState := consensus.NewConsensusState(ang.logger, conf, stateM, blockStore, mem)
	consensusState.SetPrivValidator(ang.privValidator)
	consensusReactor := consensus.NewConsensusReactor(ang.logger, consensusState, fastSync)
	consensusState.BindReactor(consensusReactor)

	bcReactor.SetBlockVerifier(func(bID types.BlockID, h int, lc *types.Commit) error {
		return stateM.Validators.VerifyCommit(stateM.ChainID, bID, h, lc)
	})
	bcReactor.SetBlockExecuter(func(blk *types.Block, pst *types.PartSet, c *types.Commit) error {
		blockStore.SaveBlock(blk, pst, c)
		if err := stateM.ApplyBlock(*ang.eventSwitch, blk, pst.Header(), MockMempool{}, -1); err != nil {
			return err
		}
		stateM.Save()
		return nil
	})

	spRouter := trace.NewRouter(ang.logger, conf, stateM, ang.PrivValidator())
	spReactor := trace.NewTraceReactor(ang.logger, conf, spRouter)
	spRouter.SetReactor(spReactor)
	spRouter.RegisterHandler(trace.SpecialOPChannel, ang.SpecialOPResponseHandler)

	privKey := ang.privValidator.GetPrivateKey()

	ang.p2pSwitch.AddReactor("MEMPOOL", memReactor)
	ang.p2pSwitch.AddReactor("BLOCKCHAIN", bcReactor)
	ang.p2pSwitch.AddReactor("CONSENSUS", consensusReactor)
	ang.p2pSwitch.AddReactor("SPECIALOP", spReactor)

	var addrBook *p2p.AddrBook
	if conf.GetBool("pex_reactor") {
		addrBook = p2p.NewAddrBook(ang.logger, conf.GetString("addrbook_file"), conf.GetBool("addrbook_strict"))
		addrBook.Start()
		pexReactor := p2p.NewPEXReactor(ang.logger, addrBook)
		ang.p2pSwitch.AddReactor("PEX", pexReactor)
	}

	if conf.GetBool("auth_by_ca") {
		ang.p2pSwitch.SetAuthByCA(authByCA(conf, stateM.ChainID, &stateM.Validators, ang.logger))
	}

	setEventSwitch(*ang.eventSwitch, bcReactor, memReactor, consensusReactor)
	initCorePlugins(stateM, privKey.(crypto.PrivKeyEd25519), ang.p2pSwitch, &stateM.Validators, ang.refuseList)

	ang.blockstore = blockStore
	ang.consensus = consensusState
	ang.mempool = mem
	ang.traceRouter = spRouter
	ang.stateMachine = stateM
	ang.addrBook = addrBook
}

func (ang *Angine) ConnectApp(app types.Application) error {
	ang.hooked = true
	hooks := app.GetAngineHooks()
	if hooks.OnExecute == nil || hooks.OnCommit == nil {
		ang.logger.Error("At least implement OnExecute & OnCommit, otherwise what your application is for?")
		return fmt.Errorf("no hooks implemented")
	}

	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookNewRound(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookNewRound)
		if hooks.OnNewRound == nil {
			data.ResCh <- types.NewRoundResult{}
			return
		}
		hooks.OnNewRound.Sync(data.Height, data.Round, nil)
		result := hooks.OnNewRound.Result()
		if r, ok := result.(types.NewRoundResult); ok {
			data.ResCh <- r
		} else {
			data.ResCh <- types.NewRoundResult{}
		}
	})
	if hooks.OnPropose != nil {
		types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookPropose(), func(ed types.TMEventData) {
			data := ed.(types.EventDataHookPropose)
			hooks.OnPropose.Async(data.Height, data.Round, nil, nil, nil)
		})
	}
	if hooks.OnPrevote != nil {
		types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookPrevote(), func(ed types.TMEventData) {
			data := ed.(types.EventDataHookPrevote)
			hooks.OnPrevote.Async(data.Height, data.Round, data.Block, nil, nil)
		})
	}
	if hooks.OnPrecommit != nil {
		types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookPrecommit(), func(ed types.TMEventData) {
			data := ed.(types.EventDataHookPrecommit)
			hooks.OnPrecommit.Async(data.Height, data.Round, data.Block, nil, nil)
		})
	}
	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookExecute(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookExecute)
		hooks.OnExecute.Sync(data.Height, data.Round, data.Block)
		result := hooks.OnExecute.Result()
		if r, ok := result.(types.ExecuteResult); ok {
			data.ResCh <- r
		} else {
			data.ResCh <- types.ExecuteResult{}
		}

	})
	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookCommit(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookCommit)
		if hooks.OnCommit == nil {
			data.ResCh <- types.CommitResult{}
			return
		}
		hooks.OnCommit.Sync(data.Height, data.Round, data.Block)
		result := hooks.OnCommit.Result()
		if cs, ok := result.(types.CommitResult); ok {
			data.ResCh <- cs
		} else {
			data.ResCh <- types.CommitResult{}
		}
	})

	if ang.genesis == nil {
		return nil
	}

	info := app.Info()
	if err := ang.RecoverFromCrash(info.LastBlockAppHash, int(info.LastBlockHeight)); err != nil {
		return err
	}

	return nil
}

func (ang *Angine) PrivValidator() *types.PrivValidator {
	return ang.privValidator
}

func (ang *Angine) Genesis() *types.GenesisDoc {
	return ang.genesis
}

func (ang *Angine) P2PHost() string {
	return ang.p2pHost
}

func (ang *Angine) P2PPort() uint16 {
	return ang.p2pPort
}

func (ang *Angine) DialSeeds(seeds []string) error {
	return ang.p2pSwitch.DialSeeds(ang.addrBook, seeds)
}

func (ang *Angine) Start() (err error) {
	ang.mtx.Lock()
	defer func() {
		ang.mtx.Unlock()
		if e := recover(); e != nil {
			err = errors.Errorf("%v", e)
		}
	}()

	if ang.started {
		return fmt.Errorf("can't start angine twice")
	}
	if !ang.hooked {
		ang.hookDefaults()
	}
	if _, err := ang.p2pSwitch.Start(); err == nil {
		ang.started = true
	} else {
		return err
	}

	seeds := ang.tune.Conf.GetString("seeds")
	if seeds != "" {
		return ang.DialSeeds(strings.Split(seeds, ","))
	}

	return nil
}

// Stop just wrap around swtich.Stop, which will stop reactors, listeners,etc
func (ang *Angine) Stop() bool {
	ret := ang.p2pSwitch.Stop()

	ang.refuseList.Stop()
	ang.statedb.Close()
	ang.blockdb.Close()
	ang.querydb.Close()

	return ret
}

func (ang *Angine) RegisterNodeInfo(ni *p2p.NodeInfo) {
	ang.p2pSwitch.SetNodeInfo(ni)
}

func (ang *Angine) GetNodeInfo() *p2p.NodeInfo {
	return ang.p2pSwitch.NodeInfo()
}

func (ang *Angine) Height() int {
	return ang.blockstore.Height()
}

func (ang *Angine) OriginHeight() int {
	return ang.blockstore.OriginHeight()
}

func (ang *Angine) NonEmptyHeight() int {
	return ang.stateMachine.LastNonEmptyHeight
}

func (ang *Angine) GetBlockMeta(height int) (meta *types.BlockMeta, err error) {

	if height == 0 {
		err = fmt.Errorf("height must be greater than 0")
		return
	}
	if height > ang.Height() {
		err = fmt.Errorf("height(%d) must be less than the current blockchain height(%d)", height, ang.Height())
		return
	}
	if height > ang.blockstore.OriginHeight() {
		meta = ang.blockstore.LoadBlockMeta(height)
	} else {
		archiveDB, errN := ang.newArchiveDB(height)
		if errN != nil {
			err = errN
			return
		}
		defer archiveDB.Close()
		newStore := blockchain.NewBlockStore(archiveDB, nil)
		meta = newStore.LoadBlockMeta(height)
	}
	return
}

func (ang *Angine) GetBlock(height int) (block *types.Block, meta *types.BlockMeta, err error) {

	if height == 0 {
		err = fmt.Errorf("height must be greater than 0")
		return
	}
	if height > ang.Height() {
		err = fmt.Errorf("height(%d) must be less than the current blockchain height(%d)", height, ang.Height())
		return
	}
	if height > ang.blockstore.OriginHeight() {
		block = ang.blockstore.LoadBlock(height)
		meta = ang.blockstore.LoadBlockMeta(height)
	} else {
		archiveDB, errN := ang.newArchiveDB(height)
		if errN != nil {
			err = errN
			return
		}
		defer archiveDB.Close()
		newStore := blockchain.NewBlockStore(archiveDB, nil)
		block = newStore.LoadBlock(height)
		meta = newStore.LoadBlockMeta(height)
	}
	return
}

func (ang *Angine) newArchiveDB(height int) (archiveDB dbm.DB, err error) {
	fileHash := string(ang.dataArchive.QueryFileHash(height))
	archiveDir := ang.conf.GetString("db_archive_dir")
	tiClient := ti.NewTiCapsuleClient(
		ang.conf.GetString("ti_endpoint"),
		ang.conf.GetString("ti_key"),
		ang.conf.GetString("ti_secret"),
	)
	_, err = os.Stat(filepath.Join(archiveDir, fileHash+".zip"))
	if err != nil {
		err = tiClient.DownloadFile(fileHash, filepath.Join(archiveDir, fileHash+".zip"))
		if err != nil {
			return
		}
		err = zip.Decompress(filepath.Join(archiveDir, fileHash+".zip"), filepath.Join(archiveDir, fileHash+".db"))
		if err != nil {
			return
		}
	}

	archiveDB = dbm.NewDB(fileHash, ang.conf.GetString("db_backend"), archiveDir)
	return
}

func (ang *Angine) GetNonEmptyBlockIterator() *blockchain.NonEmptyBlockIterator {
	return blockchain.NewNonEmptyBlockIterator(ang.blockstore)
}

func (ang *Angine) BroadcastTx(tx []byte) error {
	return ang.mempool.CheckTx(tx)
}

func (ang *Angine) BroadcastTxCommit(tx []byte) error {
	if err := ang.mempool.CheckTx(tx); err != nil {
		return err
	}
	committed := make(chan types.EventDataTx, 1)
	eventString := types.EventStringTx(tx)
	timer := time.NewTimer(60 * 2 * time.Second)
	types.AddListenerForEvent(*ang.eventSwitch, "angine", eventString, func(data types.TMEventData) {
		committed <- data.(types.EventDataTx)
	})
	defer func() {
		(*ang.eventSwitch).(events.EventSwitch).RemoveListenerForEvent(eventString, "angine")
	}()
	select {
	case res := <-committed:
		if res.Code == types.CodeType_OK {
			return nil
		}
		return fmt.Errorf(res.Error)
	case <-timer.C:
		return fmt.Errorf("Timed out waiting for transaction to be included in a block")
	}
}

func (ang *Angine) FlushMempool() {
	ang.mempool.Flush()
}

func (ang *Angine) GetValidators() (int, *types.ValidatorSet) {
	return ang.stateMachine.LastBlockHeight, ang.stateMachine.Validators
}

func (ang *Angine) GetP2PNetInfo() (bool, []string, []*types.Peer) {
	listening := ang.p2pSwitch.IsListening()
	listeners := []string{}
	for _, l := range ang.p2pSwitch.Listeners() {
		listeners = append(listeners, l.String())
	}
	peers := make([]*types.Peer, 0, ang.p2pSwitch.Peers().Size())
	for _, p := range ang.p2pSwitch.Peers().List() {
		peers = append(peers, &types.Peer{
			NodeInfo:         *p.NodeInfo,
			IsOutbound:       p.IsOutbound(),
			ConnectionStatus: p.Connection().Status(),
		})
	}
	return listening, listeners, peers
}

func (ang *Angine) GetNumPeers() int {
	o, i, d := ang.p2pSwitch.NumPeers()
	return o + i + d
}

func (ang *Angine) GetConsensusStateInfo() (string, []string) {
	roundState := ang.consensus.GetRoundState()
	peerRoundStates := make([]string, 0, ang.p2pSwitch.Peers().Size())
	for _, p := range ang.p2pSwitch.Peers().List() {
		peerState := p.Data.Get(types.PeerStateKey).(*consensus.PeerState)
		peerRoundState := peerState.GetRoundState()
		peerRoundStateStr := p.Key + ":" + string(wire.JSONBytes(peerRoundState))
		peerRoundStates = append(peerRoundStates, peerRoundStateStr)
	}
	return roundState.String(), peerRoundStates
}

func (ang *Angine) GetNumUnconfirmedTxs() int {
	return ang.mempool.Size()
}

func (ang *Angine) GetUnconfirmedTxs() []types.Tx {
	return ang.mempool.Reap(-1)
}

func (ang *Angine) IsNodeValidator(pub crypto.PubKey) bool {
	edPub := pub.(crypto.PubKeyEd25519)
	_, vals := ang.consensus.GetValidators()
	for _, v := range vals {
		if edPub.KeyString() == v.PubKey.KeyString() {
			return true
		}
	}
	return false
}

func (ang *Angine) GetBlacklist() []string {
	return ang.refuseList.ListAllKey()
}

func (ang *Angine) Query(queryType byte, load []byte) (interface{}, error) {
	return ang.QueryExecutionResult(load)
}

func (ang *Angine) QueryExecutionResult(txHash []byte) (*types.TxExecutionResult, error) {
	item := ang.querydb.Get(txHash)
	if len(item) == 0 {
		return nil, fmt.Errorf("no execution result for %v", txHash)
	}
	info := &types.TxExecutionResult{}
	if err := info.FromBytes(item); err != nil {
		return nil, err
	}
	return info, nil
}

// Recover world status
// Replay all blocks after blockHeight and ensure the result matches the current state.
func (ang *Angine) RecoverFromCrash(appHash []byte, appBlockHeight int) error {
	storeBlockHeight := ang.blockstore.Height()
	stateBlockHeight := ang.stateMachine.LastBlockHeight

	if storeBlockHeight == 0 {
		return nil // no blocks to replay
	}

	ang.logger.Info("Replay Blocks", zap.Int("appHeight", appBlockHeight), zap.Int("storeHeight", storeBlockHeight), zap.Int("stateHeight", stateBlockHeight))

	if storeBlockHeight < appBlockHeight {
		// if the app is ahead, there's nothing we can do
		return state.ErrAppBlockHeightTooHigh{CoreHeight: storeBlockHeight, AppHeight: appBlockHeight}
	} else if storeBlockHeight == appBlockHeight {
		// We ran Commit, but if we crashed before state.Save(),
		// load the intermediate state and update the state.AppHash.
		// NOTE: If ABCI allowed rollbacks, we could just replay the
		// block even though it's been committed
		stateAppHash := ang.stateMachine.AppHash
		lastBlockAppHash := ang.blockstore.LoadBlock(storeBlockHeight).AppHash

		if bytes.Equal(stateAppHash, appHash) {
			// we're all synced up
			ang.logger.Debug("RelpayBlocks: Already synced")
		} else if bytes.Equal(stateAppHash, lastBlockAppHash) {
			// we crashed after commit and before saving state,
			// so load the intermediate state and update the hash
			if err := ang.stateMachine.LoadIntermediate(); err != nil {
				return err
			}
			ang.stateMachine.AppHash = appHash
			ang.logger.Debug("RelpayBlocks: Loaded intermediate state and updated state.AppHash")
		} else {
			return errors.Errorf("Unexpected state.AppHash: state.AppHash %X; app.AppHash %X, lastBlock.AppHash %X", stateAppHash, appHash, lastBlockAppHash)
		}

		return nil
	} else if storeBlockHeight == appBlockHeight+1 &&
		storeBlockHeight == stateBlockHeight+1 {
		// We crashed after saving the block
		// but before Commit (both the state and app are behind),
		// so just replay the block

		// check that the lastBlock.AppHash matches the state apphash
		block := ang.blockstore.LoadBlock(storeBlockHeight)
		if !bytes.Equal(block.Header.AppHash, appHash) {
			return state.ErrLastStateMismatch{Height: storeBlockHeight, Core: block.Header.AppHash, App: appHash}
		}

		blockMeta := ang.blockstore.LoadBlockMeta(storeBlockHeight)
		// h.nBlocks++
		// replay the latest block
		return ang.stateMachine.ApplyBlock(*ang.eventSwitch, block, blockMeta.PartsHeader, MockMempool{}, 0)
	} else if storeBlockHeight != stateBlockHeight {
		// unless we failed before committing or saving state (previous 2 case),
		// the store and state should be at the same height!
		if storeBlockHeight == stateBlockHeight+1 {
			ang.stateMachine.AppHash = appHash
			ang.stateMachine.LastBlockHeight = storeBlockHeight
			ang.stateMachine.LastBlockID = ang.blockstore.LoadBlockMeta(storeBlockHeight).Header.LastBlockID
			ang.stateMachine.LastBlockTime = ang.blockstore.LoadBlockMeta(storeBlockHeight).Header.Time
		} else {
			return errors.Errorf("Expected storeHeight (%d) and stateHeight (%d) to match.", storeBlockHeight, stateBlockHeight)
		}
	} else {
		// store is more than one ahead,
		// so app wants to replay many blocks
		// replay all blocks starting with appBlockHeight+1
		// var eventCache types.Fireable // nil
		// TODO: use stateBlockHeight instead and let the consensus state do the replay
		for h := appBlockHeight + 1; h <= storeBlockHeight; h++ {
			// h.nBlocks++
			block := ang.blockstore.LoadBlock(h)
			blockMeta := ang.blockstore.LoadBlockMeta(h)
			if err := ang.stateMachine.ApplyBlock(*ang.eventSwitch, block, blockMeta.PartsHeader, MockMempool{}, 0); err != nil {
				return errors.Wrap(err, "fail to apply block during recovery")
			}
		}
		if !bytes.Equal(ang.stateMachine.AppHash, appHash) {
			return fmt.Errorf("Ann state.AppHash does not match AppHash after replay. Got %X, expected %X", appHash, ang.stateMachine.AppHash)
		}
	}

	return nil
}

func (ang *Angine) hookDefaults() {
	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookNewRound(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookNewRound)
		data.ResCh <- types.NewRoundResult{}
	})
	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookExecute(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookExecute)
		data.ResCh <- types.ExecuteResult{}
	})
	types.AddListenerForEvent(*ang.eventSwitch, "angine", types.EventStringHookCommit(), func(ed types.TMEventData) {
		data := ed.(types.EventDataHookCommit)
		data.ResCh <- types.CommitResult{}
	})
}

func setEventSwitch(evsw types.EventSwitch, eventables ...types.Eventable) {
	for _, e := range eventables {
		e.SetEventSwitch(evsw)
	}
}

func addToRefuselist(refuseList *refuse_list.RefuseList) func([32]byte) error {
	return func(pk [32]byte) error {
		refuseList.AddRefuseKey(pk)
		return nil
	}
}

func refuseListFilter(refuseList *refuse_list.RefuseList) func(crypto.PubKeyEd25519) error {
	return func(pubkey crypto.PubKeyEd25519) error {
		if refuseList.QueryRefuseKey(pubkey) {
			return fmt.Errorf("%s in refuselist", pubkey.KeyString())
		}
		return nil
	}
}

func authByCA(conf *viper.Viper, chainID string, ppValidators **types.ValidatorSet, log *zap.Logger) func(*p2p.NodeInfo) error {
	valset := *ppValidators
	chainIDBytes := []byte(chainID)
	return func(peerNodeInfo *p2p.NodeInfo) error {
		// validator node must be signed by CA
		// but normal node can bypass auth check if config says so
		if !valset.HasAddress(peerNodeInfo.PubKey.Address()) && !conf.GetBool("non_validator_node_auth") {
			return nil
		}
		msg := append(peerNodeInfo.PubKey[:], chainIDBytes...)
		for _, val := range valset.Validators {
			// only CA
			if !val.IsCA {
				continue
			}
			valPk := [32]byte(val.PubKey.(crypto.PubKeyEd25519))
			signedPkByte64, err := types.StringTo64byte(peerNodeInfo.SigndPubKey)
			if err != nil {
				return err
			}
			if ed25519.Verify(&valPk, msg, &signedPkByte64) {
				log.Sugar().Infow("Peer handshake", "peerNodeInfo", peerNodeInfo)
				return nil
			}
		}
		err := fmt.Errorf("Reject Peer, has no CA sig")
		log.Warn(err.Error())
		return err
	}
}

func initCorePlugins(sm *state.State, privkey crypto.PrivKeyEd25519, sw *p2p.Switch, ppValset **types.ValidatorSet, rl *refuse_list.RefuseList) {
	params := &plugin.InitPluginParams{
		Switch:     sw,
		PrivKey:    privkey,
		RefuseList: rl,
		Validators: ppValset,
	}
	for _, plug := range sm.Plugins {
		plug.InitPlugin(params)
	}
}

func fastSyncable(conf *viper.Viper, selfAddress []byte, validators *types.ValidatorSet) bool {
	// We don't fast-sync when the only validator is us.
	fastSync := conf.GetBool("fast_sync")
	if validators.Size() == 1 {
		addr, _ := validators.GetByIndex(0)
		if bytes.Equal(selfAddress, addr) {
			fastSync = false
		}
	}
	return fastSync
}

func getGenesisFile(conf *viper.Viper) (*types.GenesisDoc, error) {
	genDocFile := conf.GetString("genesis_file")
	if !cmn.FileExists(genDocFile) {
		return nil, fmt.Errorf("missing genesis_file")
	}
	jsonBlob, err := ioutil.ReadFile(genDocFile)
	if err != nil {
		return nil, fmt.Errorf("Couldn't read GenesisDoc file: %v", err)
	}
	genDoc := types.GenesisDocFromJSON(jsonBlob)
	if genDoc.ChainID == "" {
		return nil, fmt.Errorf("Genesis doc %v must include non-empty chain_id", genDocFile)
	}
	conf.Set("chain_id", genDoc.ChainID)

	return genDoc, nil
}

func getLogger(conf *viper.Viper, chainID string) (*zap.Logger, error) {
	logpath := conf.GetString("log_path")
	if logpath == "" {
		logpath, _ = os.Getwd()
	}
	logpath = path.Join(logpath, "angine-"+chainID)
	if err := cmn.EnsureDir(logpath, 0700); err != nil {
		return nil, err
	}
	if logger := InitializeLog(conf.GetString("environment"), logpath); logger != nil {
		return logger, nil
	}
	return nil, fmt.Errorf("fail to build zap logger")
}

func genPrivFile(path string) *types.PrivValidator {
	privValidator := types.GenPrivValidator(nil)
	privValidator.SetFile(path)
	privValidator.Save()
	return privValidator
}

func genGenesiFile(path, chainID string, gVals []types.GenesisValidator) (*types.GenesisDoc, error) {
	if len(chainID) == 0 {
		chainID = cmn.Fmt("annchain-%v", cmn.RandStr(6))
	}
	genDoc := &types.GenesisDoc{
		ChainID: chainID,
		Plugins: "specialop",
	}
	genDoc.Validators = gVals
	return genDoc, genDoc.SaveAs(path)
}

func checkPrivValidatorFile(conf *viper.Viper) error {
	if privFile := conf.GetString("priv_validator_file"); !cmn.FileExists(privFile) {
		return fmt.Errorf("PrivValidator file needed: %s", privFile)
	}
	return nil
}

func checkGenesisFile(conf *viper.Viper) error {
	if genFile := conf.GetString("genesis_file"); !cmn.FileExists(genFile) {
		return fmt.Errorf("Genesis file needed: %s", genFile)
	}
	return nil
}

func ensureQueryDB(dbDir string) (*dbm.GoLevelDB, error) {
	if err := cmn.EnsureDir(path.Join(dbDir, "query_cache"), 0775); err != nil {
		return nil, fmt.Errorf("fail to ensure tx_execution_result")
	}
	querydb, err := dbm.NewGoLevelDB("tx_execution_result", path.Join(dbDir, "query_cache"))
	if err != nil {
		return nil, fmt.Errorf("fail to open tx_execution_result")
	}
	return querydb, nil
}

func getOrMakeState(logger *zap.Logger, conf *viper.Viper, stateDB dbm.DB, genesis *types.GenesisDoc) (*state.State, error) {
	stateM := state.GetState(logger, conf, stateDB)
	if stateM == nil {
		if genesis != nil {
			if stateM = state.MakeGenesisState(logger, stateDB, genesis); stateM == nil {
				return nil, fmt.Errorf("fail to get genesis state")
			}
		}
	}
	return stateM, nil
}

func prepareP2P(logger *zap.Logger, conf *viper.Viper, genesisBytes []byte, privValidator *types.PrivValidator, refuseList *refuse_list.RefuseList) (*p2p.Switch, error) {
	p2psw := p2p.NewSwitch(logger, conf, genesisBytes)
	protocol, address := ProtocolAndAddress(conf.GetString("p2p_laddr"))
	defaultListener, err := p2p.NewDefaultListener(logger, protocol, address, conf.GetBool("skip_upnp"))
	if err != nil {
		return nil, errors.Wrap(err, "prepareP2P")
	}
	nodeInfo := &p2p.NodeInfo{
		PubKey:      privValidator.PubKey.(crypto.PubKeyEd25519),
		SigndPubKey: conf.GetString("signbyCA"),
		Moniker:     conf.GetString("moniker"),
		ListenAddr:  defaultListener.ExternalAddress().String(),
		Version:     version,
	}
	privKey := privValidator.PrivKey
	p2psw.AddListener(defaultListener)
	p2psw.SetNodeInfo(nodeInfo)
	p2psw.SetNodePrivKey(privKey.(crypto.PrivKeyEd25519))
	p2psw.SetAddToRefuselist(addToRefuselist(refuseList))
	p2psw.SetRefuseListFilter(refuseListFilter(refuseList))

	return p2psw, nil
}

// --------------------------------------------------------------------------------

// Updates to the mempool need to be synchronized with committing a block
// so apps can reset their transient state on Commit
type MockMempool struct {
}

func (m MockMempool) Lock()                               {}
func (m MockMempool) Unlock()                             {}
func (m MockMempool) Update(height int64, txs []types.Tx) {}

type ITxCheck interface {
	CheckTx(types.Tx) (bool, error)
}
type MempoolFilter struct {
	cb func([]byte) (bool, error)
}

func (m MempoolFilter) CheckTx(tx types.Tx) (bool, error) {
	return m.cb(tx)
}

func NewMempoolFilter(f func([]byte) (bool, error)) MempoolFilter {
	return MempoolFilter{cb: f}
}
