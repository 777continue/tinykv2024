package standalone_storage

import (
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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.txn, cf, key)
}
func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}
func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.db, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.db, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		default:
			log.Error("unknown type")
		}
	}
	return nil
}
