package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf   *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := path.Join(conf.DBPath, "kv")
	raftPath := path.Join(conf.DBPath, "raft")
	s := &StandAloneStorage{
		conf:   conf,
		engine: engine_util.NewEngines(engine_util.CreateDB(kvPath, false), engine_util.CreateDB(raftPath, true), kvPath, raftPath),
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.SetFlags(log.LstdFlags)
	log.SetLevel(log.LOG_LEVEL_DEBUG)
	log.Debugf("StandAloneStorage Starting")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	log.Debugf("StandAloneStorage Stopping")
	if err := s.engine.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	log.Debugf("Reader Generating")
	txn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 这里选择把batch的修改存入writeBatch后一次性writeKV
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			log.Debugf("Put | CF: %v, Key: %v, Value: %v", data.Cf, data.Key, data.Value)
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			log.Debugf("Delete | CF: %v, Key: %v", data.Cf, data.Key)
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	if err := s.engine.WriteKV(wb); err != nil {
		log.Errorf("Write Error :%v", err)
		return err
	}
	return nil
}

type StandAloneReader struct {
	iter *engine_util.BadgerIterator
	txn  *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	sr := &StandAloneReader{
		iter: nil,
		txn:  txn,
	}
	return sr
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) (val []byte, err error) {
	// 在storage层就要把badger的not found err给兜住，不然过不了测试TestRawDelete1
	log.Debugf("Reader GetCF | CF: %v, Key: %v", cf, key)
	val, err = engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		log.Infof("GetCF KeyNotFoundError | CF: %v, Key: %v", cf, key)
		err = nil
	}
	return val, err
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	log.Debugf("Reader Generate Iterator | CF:%v", cf)
	sr.iter = engine_util.NewCFIterator(cf, sr.txn)
	return sr.iter
}

func (sr *StandAloneReader) Close() {
	log.Debugf("Reader Close Iterator")
	// 这里应该是要有个迭代器对象的，因为迭代器也需要close
	sr.iter.Close()
	sr.txn.Discard()
}
