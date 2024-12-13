package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(req.Context)
	val, err := reader.GetCF(req.Cf, req.Key)
	var noyfound bool
	if err != nil {
		noyfound = true
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: noyfound,
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	},
	})
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	},
	})
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(req.Context)
	iter := reader.IterCF(req.Cf)
	limit := req.GetLimit()
	resp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0),
	}
	for iter.Seek(req.StartKey); iter.Valid() && limit > 0; iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		limit--
	}
	return resp, nil
}
