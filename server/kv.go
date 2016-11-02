// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"math"
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"golang.org/x/net/context"
)

const (
	kvRangeLimit = 100000
)

var (
	errTxnFailed    = errors.New("failed to commit transaction")
	errBootstrapped = errors.New("cluster is already bootstrapped")
)

// kv wraps all kv operations, keep it stateless.
type kv struct {
	s           *Server
	clusterPath string
}

func newKV(s *Server) *kv {
	return &kv{
		s:           s,
		clusterPath: path.Join(s.rootPath, "raft"),
	}
}

func (kv *kv) client() *clientv3.Client            { return kv.s.client }
func (kv *kv) txn(cs ...clientv3.Cmp) clientv3.Txn { return kv.s.leaderTxn(cs...) }

func (kv *kv) storePath(storeID uint64) string {
	return path.Join(kv.clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func (kv *kv) regionPath(regionID uint64) string {
	return path.Join(kv.clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

func (kv *kv) bootstrapCluster(store *metapb.Store, region *metapb.Region) error {
	cluster := &metapb.Cluster{
		Id:           kv.s.cfg.ClusterID,
		MaxPeerCount: uint32(kv.s.cfg.MaxPeerCount),
	}

	var ops []clientv3.Op

	// Put cluster.
	value, err := cluster.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(kv.clusterPath, string(value)))

	// Put store.
	value, err = store.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(kv.storePath(store.GetId()), string(value)))

	// Put region.
	value, err = region.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(kv.regionPath(region.GetId()), string(value)))

	cmp := clientv3.Compare(clientv3.CreateRevision(kv.clusterPath), "=", 0)
	resp, err := kv.txn(cmp).Then(ops...).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errBootstrapped)
	}
	return nil
}

func (kv *kv) initCluster() (*clusterInfo, error) {
	log.Info("load cluster")

	cache := newClusterInfo(kv.s.idAlloc)
	cache.meta = &metapb.Cluster{}

	ok, err := kv.loadMeta(cache.meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := kv.loadStores(cache.stores, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v stores cost %v", cache.getStoreCount(), time.Since(start))

	start = time.Now()
	if err := kv.loadRegions(cache.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v regions cost %v", cache.getRegionCount(), time.Since(start))

	return cache, nil
}

func (kv *kv) loadStores(stores *storesInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endStore := kv.storePath(math.MaxUint64)
	withRange := clientv3.WithRange(endStore)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.storePath(nextID)
		resp, err := kvGet(kv.client(), key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		for _, item := range resp.Kvs {
			store := &metapb.Store{}
			if err := store.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}

			nextID = store.GetId() + 1
			stores.setStore(newStoreInfo(store))
		}
	}
}

func (kv *kv) loadRegions(regions *regionsInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endRegion := kv.regionPath(math.MaxUint64)
	withRange := clientv3.WithRange(endRegion)
	withLimit := clientv3.WithLimit(rangeLimit)

	for {
		key := kv.regionPath(nextID)
		resp, err := kvGet(kv.client(), key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		for _, item := range resp.Kvs {
			region := &metapb.Region{}
			if err := region.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			regions.setRegion(newRegionInfo(region, nil))
		}
	}
}

func (kv *kv) loadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(kv.clusterPath, meta)
}

func (kv *kv) saveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(kv.clusterPath, meta)
}

func (kv *kv) loadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return kv.loadProto(kv.storePath(storeID), store)
}

func (kv *kv) saveStore(store *metapb.Store) error {
	return kv.saveProto(kv.storePath(store.GetId()), store)
}

func (kv *kv) loadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.loadProto(kv.regionPath(regionID), region)
}

func (kv *kv) saveRegion(region *metapb.Region) error {
	return kv.saveProto(kv.regionPath(region.GetId()), region)
}

func (kv *kv) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	return true, proto.Unmarshal(value, msg)
}

func (kv *kv) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.save(key, string(value))
}

func (kv *kv) load(key string) ([]byte, error) {
	resp, err := kvGet(kv.client(), key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return resp.Kvs[0].Value, nil
}

func (kv *kv) save(key, value string) error {
	resp, err := kv.txn().Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if cost := time.Now().Sub(start); cost > slowRequestTime {
		log.Warnf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}

	return resp, errors.Trace(err)
}
