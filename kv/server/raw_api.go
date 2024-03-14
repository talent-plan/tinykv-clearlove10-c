package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Warnf("Reader Generating Error: %v", err)
		return nil, err
	}
	var resp *kvrpcpb.RawGetResponse
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		log.Debugf("Reader GetCF Error: %v", err)
		resp = &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}
		return resp, err
	}
	if value == nil {
		resp = &kvrpcpb.RawGetResponse{
			NotFound: true,
		}
		return resp, nil
	}
	resp = &kvrpcpb.RawGetResponse{
		NotFound: false,
		Value:    value,
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	m := &storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{*m})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	m := &storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{*m})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	if !iter.Valid() {
		return nil, nil
	}
	var kvs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.GetLimit(); i++ {
		k := iter.Item().Key()
		v, err := iter.Item().Value()
		if err != nil {
			return nil, nil
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		iter.Next()
		if !iter.Valid() {
			break
		}
	}
	reader.Close()
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
