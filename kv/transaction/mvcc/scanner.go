package mvcc

import (
	"bytes"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter     engine_util.DBIterator
	txn      *MvccTxn
	startKey []byte
	finish   bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		txn:      txn,
		iter:     txn.Reader.IterCF(engine_util.CfWrite),
		startKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.finish {
		return nil, nil, nil
	}
	scankey := scan.startKey
	scan.iter.Seek(EncodeKey(scankey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	wkey := scan.iter.Item().KeyCopy(nil)
	key := DecodeUserKey(wkey)
	if !bytes.Equal(key, scankey) {
		scan.startKey = key
		return scan.Next()
	}
	wval, err := scan.iter.Item().ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.finish = true
			break
		}
		nextWKey := scan.iter.Item().KeyCopy(nil)
		nextKey := DecodeUserKey(nextWKey)
		if !bytes.Equal(key, nextKey) {
			scan.startKey = nextKey
			break
		}
	}
	write, err := ParseWrite(wval)
	if err != nil {
		return nil, nil, err
	}
	if write.Kind == WriteKindPut {
		val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		fmt.Printf("scan.Next : key : %d, val : %d\n", key, val)
		return key, val, err
	}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	return nil, nil, nil
}
