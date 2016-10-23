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
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

// Create n stores (0..n).
func newTestStores(n uint64) []*storeInfo {
	stores := make([]*storeInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, newStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) Test(c *C) {
	n := uint64(10)
	cache := newStoresInfo()
	stores := newTestStores(n)

	for i := uint64(0); i < n; i++ {
		c.Assert(cache.getStore(i), IsNil)
		cache.setStore(stores[i])
		c.Assert(cache.getStore(i), DeepEquals, stores[i])
		c.Assert(cache.getStoreCount(), Equals, int(i+1))
	}
	c.Assert(cache.getStoreCount(), Equals, int(n))

	for _, store := range cache.getStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	for _, store := range cache.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()].Store)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct{}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*regionInfo {
	regions := make([]*regionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:       i,
			Peers:    peers,
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		regions = append(regions, newRegionInfo(region, peers[0]))
	}
	return regions
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	cache := newRegionsInfo()
	regions := newTestRegions(n, np)

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.getRegion(i), IsNil)
		c.Assert(cache.searchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.addRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegion(c, cache.searchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		// Update leader to peer np-1.
		region.Leader = region.Peers[np-1]
		cache.updateRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegion(c, cache.searchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.removeRegion(region.GetId())
		c.Assert(cache.getRegion(i), IsNil)
		c.Assert(cache.searchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		region.Leader = region.Peers[0]
		cache.addRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.searchRegion(regionKey), region)
	}

	for i := uint64(0); i < n; i++ {
		region := cache.randLeaderRegion(i)
		c.Assert(region.Leader.GetStoreId(), Equals, i)

		region = cache.randFollowerRegion(i)
		c.Assert(region.Leader.GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}
}

func checkRegion(c *C, a *regionInfo, b *regionInfo) {
	c.Assert(a.Region, DeepEquals, b.Region)
	c.Assert(a.Leader, DeepEquals, b.Leader)
}

func checkRegions(c *C, cache *regionsInfo, regions []*regionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.Peers {
			regionCount[peer.StoreId]++
			if peer.Id == region.Leader.Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.leaders[peer.StoreId][region.Id], region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.followers[peer.StoreId][region.Id], region)
			}
		}
	}

	c.Assert(cache.getRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.getStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.getStoreLeaderCount(id), Equals, count)
	}
	for id, count := range followerCount {
		c.Assert(cache.getStoreFollowerCount(id), Equals, count)
	}

	for _, region := range cache.getRegions() {
		checkRegion(c, region, regions[region.GetId()])
	}
	for _, region := range cache.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) TestStoreHeartbeat(c *C) {
	n, np := uint64(3), uint64(3)
	cache := newClusterInfo(newMockIDAllocator())
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		cache.addRegion(region)
	}
	c.Assert(cache.getRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{StoreId: store.GetId()}
		c.Assert(cache.handleStoreHeartbeat(storeStats), NotNil)

		cache.setStore(store)
		c.Assert(cache.getStoreCount(), Equals, int(i+1))

		stats := store.stats
		startTS := stats.StartTS
		c.Assert(stats.StartTS.IsZero(), IsFalse)
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsTrue)
		c.Assert(stats.TotalRegionCount, Equals, 0)
		c.Assert(stats.LeaderRegionCount, Equals, 0)

		c.Assert(cache.handleStoreHeartbeat(storeStats), IsNil)

		stats = cache.getStore(store.GetId()).stats
		c.Assert(stats.StartTS, Equals, startTS)
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsFalse)
		c.Assert(stats.TotalRegionCount, Equals, int(n))
		c.Assert(stats.LeaderRegionCount, Equals, 1)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))
}

func (s *testClusterInfoSuite) TestRegionHeartbeat(c *C) {
	n, np := uint64(3), uint64(3)
	cache := newClusterInfo(newMockIDAllocator())
	regions := newTestRegions(n, np)

	for i, region := range regions {
		// region does not exist.
		updated, err := cache.handleRegionHeartbeat(region)
		c.Assert(updated, IsTrue)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is the same, not updated.
		updated, err = cache.handleRegionHeartbeat(region)
		c.Assert(updated, IsFalse)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		epoch := region.clone().GetRegionEpoch()

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		updated, err = cache.handleRegionHeartbeat(region)
		c.Assert(updated, IsTrue)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is stale (Version).
		stale := region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: epoch.GetConfVer() + 1,
		}
		updated, err = cache.handleRegionHeartbeat(stale)
		c.Assert(updated, IsFalse)
		c.Assert(err, NotNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
			ConfVer: epoch.GetConfVer() + 1,
		}
		updated, err = cache.handleRegionHeartbeat(region)
		c.Assert(updated, IsTrue)
		c.Assert(err, IsNil)
		checkRegions(c, cache.regions, regions[0:i+1])

		// region is stale (ConfVer).
		stale = region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		updated, err = cache.handleRegionHeartbeat(stale)
		c.Assert(updated, IsFalse)
		c.Assert(err, NotNil)
		checkRegions(c, cache.regions, regions[0:i+1])
	}
}

func heartbeatRegions(c *C, cache *clusterInfo, regions []*metapb.Region) {
	// Heartbeat and check region one by one.
	for _, region := range regions {
		r := newRegionInfo(region, nil)

		updated, err := cache.handleRegionHeartbeat(r)
		c.Assert(updated, IsTrue)
		c.Assert(err, IsNil)

		checkRegion(c, cache.getRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, region := range regions {
		r := newRegionInfo(region, nil)

		checkRegion(c, cache.getRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
			result := cache.searchRegion([]byte{end + 1})
			c.Assert(result.GetId(), Not(Equals), r.GetId())
		}
	}
}

func (s *testClusterInfoSuite) TestRegionHeartbeatSplitAndMerge(c *C) {
	cache := newClusterInfo(newMockIDAllocator())
	regions := []*metapb.Region{
		{
			Id:          1,
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = splitRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = mergeRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = mergeRegions(regions)
		} else {
			regions = splitRegions(regions)
		}
		heartbeatRegions(c, cache, regions)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := newRegion([]byte{}, []byte{})
	origin := newRegion([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region, origin), IsNil)
	c.Assert(checkStaleRegion(origin, region), IsNil)

	// (1, 0) v.s. (0, 0)
	region.RegionEpoch.Version++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (1, 1) v.s. (0, 0)
	region.RegionEpoch.ConfVer++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (0, 1) v.s. (0, 0)
	region.RegionEpoch.Version--
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)
}

// TODO: Move this to some where else.
// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{base: 0}
}

func (alloc *mockIDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}
