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
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type testClusterInfo struct {
	*clusterInfo
}

func newTestClusterInfo(cluster *clusterInfo) *testClusterInfo {
	return &testClusterInfo{clusterInfo: cluster}
}

func (c *testClusterInfo) setStoreUp(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.stats.LastHeartbeatTS = time.Now()
	c.putStore(store)
}

func (c *testClusterInfo) setStoreDown(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Up
	store.stats.LastHeartbeatTS = time.Time{}
	c.putStore(store)
}

func (c *testClusterInfo) setStoreOffline(storeID uint64) {
	store := c.getStore(storeID)
	store.State = metapb.StoreState_Offline
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderStore(storeID uint64, leaderCount, regionCount int) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.TotalRegionCount = regionCount
	store.stats.LeaderRegionCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) addRegionStore(storeID uint64, regionCount int, storageRatio float64) {
	store := newStoreInfo(&metapb.Store{Id: storeID})
	store.stats.LastHeartbeatTS = time.Now()
	store.stats.RegionCount = uint32(regionCount)
	store.stats.Capacity = 100
	store.stats.Available = uint64((1 - storageRatio) * float64(store.stats.Capacity))
	c.putStore(store)
}

func (c *testClusterInfo) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := c.allocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := c.allocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	c.putRegion(newRegionInfo(region, leader))
}

func (c *testClusterInfo) updateLeaderCount(storeID uint64, leaderCount, regionCount int) {
	store := c.getStore(storeID)
	store.stats.LeaderRegionCount = leaderCount
	c.putStore(store)
}

func (c *testClusterInfo) updateRegionCount(storeID uint64, regionCount int, storageRatio float64) {
	store := c.getStore(storeID)
	store.stats.RegionCount = uint32(regionCount)
	store.stats.Capacity = 100
	store.stats.Available = uint64((1 - storageRatio) * float64(store.stats.Capacity))
	c.putStore(store)
}

func (c *testClusterInfo) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := c.getStore(storeID)
	store.stats.ApplyingSnapCount = uint32(snapshotCount)
	c.putStore(store)
}

func newTestScheduleConfig() (*ScheduleConfig, *scheduleOption) {
	cfg := newScheduleConfig()
	cfg.adjust()
	opt := newScheduleOption(cfg)
	return cfg, opt
}

var _ = Suite(&testLeaderBalancerSuite{})

type testLeaderBalancerSuite struct{}

func (s *testLeaderBalancerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	lb := newLeaderBalancer(opt)

	cfg.MinLeaderCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	tc.addLeaderStore(1, 6, 30)
	tc.addLeaderStore(2, 7, 30)
	tc.addLeaderStore(3, 8, 30)
	tc.addLeaderStore(4, 9, 30)
	tc.addLeaderRegion(1, 4, 1, 2, 3)

	// Test leaderCountFilter.
	c.Assert(lb.Schedule(cluster), IsNil)
	tc.updateLeaderCount(4, 11, 30)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreDown(1)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	tc.updateLeaderCount(2, 10, 30)
	tc.updateLeaderCount(3, 10, 30)
	tc.updateLeaderCount(4, 11, 30)
	c.Assert(lb.Schedule(cluster), IsNil)
}

var _ = Suite(&testStorageBalancerSuite{})

type testStorageBalancerSuite struct{}

func (s *testStorageBalancerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	sb := newStorageBalancer(opt)

	cfg.MinRegionCount = 10
	cfg.MinBalanceDiffRatio = 0.1

	tc.addRegionStore(1, 6, 0.1)
	tc.addRegionStore(2, 7, 0.2)
	tc.addRegionStore(3, 8, 0.3)
	tc.addRegionStore(4, 9, 0.4)
	tc.addLeaderRegion(1, 4)

	// Test regionCountFilter.
	c.Assert(sb.Schedule(cluster), IsNil)
	tc.updateRegionCount(4, 11, 0.4)
	checkTransferPeer(c, sb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	tc.updateRegionCount(2, 6, 0.4)
	tc.updateRegionCount(3, 7, 0.4)
	tc.updateRegionCount(4, 8, 0.4)
	c.Assert(sb.Schedule(cluster), IsNil)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	rc := newReplicaChecker(cluster, opt)

	cfg.MaxSnapshotCount = 2
	cluster.putMeta(&metapb.Cluster{Id: 1, MaxPeerCount: 3})

	tc.addRegionStore(1, 4, 0.4)
	tc.addRegionStore(2, 3, 0.3)
	tc.addRegionStore(3, 2, 0.1)
	tc.addRegionStore(4, 1, 0.2)
	tc.addLeaderRegion(1, 1, 2)

	// Region has 2 peers, we need 3.
	region := cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 2 is down, add peer in store 4.
	downPeer := &pdpb.PeerStats{
		Peer:        region.GetStorePeer(2),
		DownSeconds: proto.Uint64(24 * 60 * 60),
	}
	region.DownPeers = append(region.DownPeers, downPeer)
	checkAddPeer(c, rc.Check(region), 4)
	region.DownPeers = nil
	c.Assert(rc.Check(region), IsNil)

	// Peer in store 1 is offline, add peer in store 4.
	tc.setStoreOffline(1)
	checkAddPeer(c, rc.Check(region), 4)

	// Test stateFilter.
	tc.setStoreDown(4)
	c.Assert(rc.Check(region), IsNil)

	// Test snapshotCountFilter.
	tc.setStoreUp(4)
	checkAddPeer(c, rc.Check(region), 4)
	tc.updateSnapshotCount(4, 3)
	c.Assert(rc.Check(region), IsNil)
	tc.updateSnapshotCount(4, 1)
	checkAddPeer(c, rc.Check(region), 4)

	// Remove redundant peer.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 1)
}

func checkAddPeer(c *C, bop *balanceOperator, storeID uint64) {
	op := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkRemovePeer(c *C, bop *balanceOperator, storeID uint64) {
	op := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, storeID)
}

func checkTransferPeer(c *C, bop *balanceOperator, sourceID, targetID uint64) {
	op := bop.Ops[0].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, targetID)
	op = bop.Ops[1].(*changePeerOperator)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, sourceID)
}

func checkTransferLeader(c *C, bop *balanceOperator, sourceID, targetID uint64) {
	op := bop.Ops[0].(*transferLeaderOperator)
	c.Assert(op.OldLeader.GetStoreId(), Equals, sourceID)
	c.Assert(op.NewLeader.GetStoreId(), Equals, targetID)
}

var _ = Suite(&testBalancerSuite{})

type testBalancerSuite struct {
	testClusterBaseSuite

	cfg *ScheduleConfig
	opt *scheduleOption
}

func (s *testBalancerSuite) getRootPath() string {
	return "test_balancer"
}

func (s *testBalancerSuite) SetUpSuite(c *C) {
	s.cfg = newScheduleConfig()
	s.cfg.adjust()
	s.opt = newScheduleOption(s.cfg)
}

func (s *testBalancerSuite) newClusterInfo(c *C) *clusterInfo {
	clusterInfo := newClusterInfo(newMockIDAllocator())

	// Set cluster info.
	meta := &metapb.Cluster{
		Id:           0,
		MaxPeerCount: 3,
	}
	clusterInfo.putMeta(meta)

	var (
		id   uint64
		peer *metapb.Peer
		err  error
	)

	// Add 4 stores, store id will be 1,2,3,4.
	for i := 1; i < 5; i++ {
		id, err = clusterInfo.allocID()
		c.Assert(err, IsNil)

		addr := fmt.Sprintf("127.0.0.1:%d", i)
		store := s.newStore(c, id, addr)
		clusterInfo.putStore(newStoreInfo(store))
	}

	// Add 1 peer, id will be 5.
	id, err = clusterInfo.allocID()
	c.Assert(err, IsNil)
	peer = s.newPeer(c, 1, id)

	// Add 1 region, id will be 6.
	id, err = clusterInfo.allocID()
	c.Assert(err, IsNil)

	region := s.newRegion(c, id, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)
	clusterInfo.putRegion(newRegionInfo(region, peer))

	stores := clusterInfo.getStores()
	c.Assert(stores, HasLen, 4)

	return clusterInfo
}

func (s *testBalancerSuite) updateStore(c *C, clusterInfo *clusterInfo, storeID uint64, capacity uint64, available uint64,
	sendingSnapCount uint32, receivingSnapCount uint32, applyingSnapCount uint32) {
	stats := &pdpb.StoreStats{
		StoreId:            storeID,
		Capacity:           capacity,
		Available:          available,
		SendingSnapCount:   sendingSnapCount,
		ReceivingSnapCount: receivingSnapCount,
		ApplyingSnapCount:  applyingSnapCount,
	}

	c.Assert(clusterInfo.handleStoreHeartbeat(stats), IsNil)
}

func (s *testBalancerSuite) updateStoreState(c *C, clusterInfo *clusterInfo, storeID uint64, state metapb.StoreState) {
	store := clusterInfo.getStore(storeID)
	store.State = state
	clusterInfo.putStore(store)
}

func (s *testBalancerSuite) addRegionPeer(c *C, clusterInfo *clusterInfo, storeID uint64, region *regionInfo) {
	r := newReplicaChecker(clusterInfo, s.opt)
	bop := r.Check(region)
	c.Assert(bop, NotNil)

	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
	c.Assert(ok, IsTrue)
	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)

	peer := op.ChangePeer.GetPeer()
	c.Assert(peer.GetStoreId(), Equals, storeID)

	addRegionPeer(c, region.Region, peer)

	clusterInfo.putRegion(region)
}

// func (s *testBalancerSuite) TestCapacityBalancer(c *C) {
// 	clusterInfo := s.newClusterInfo(c)
// 	c.Assert(clusterInfo, NotNil)

// 	region := clusterInfo.searchRegion([]byte("a"))
// 	c.Assert(region.GetPeers(), HasLen, 1)

// 	// The store id will be 1,2,3,4.
// 	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0, 0)

// 	// Now we have all stores with low capacity used ratio, so we need not to do balance.
// 	testCfg := newBalanceConfig()
// 	testCfg.adjust()
// 	testCfg.MinCapacityUsedRatio = 0.5
// 	testCfg.MaxCapacityUsedRatio = 0.9
// 	cb := newCapacityBalancer(testCfg)
// 	_, bop, err := cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Now region peer count is less than max peer count, so we need not to do balance.
// 	testCfg.MinCapacityUsedRatio = 0.1
// 	testCfg.MaxCapacityUsedRatio = 0.9

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Get leader peer.
// 	leader := region.GetPeers()[0]
// 	c.Assert(leader, NotNil)

// 	// Add two peers.
// 	s.addRegionPeer(c, clusterInfo, 4, region)
// 	s.addRegionPeer(c, clusterInfo, 3, region)

// 	// If we cannot find a `from store` to do balance, then we will do nothing.
// 	s.updateStore(c, clusterInfo, 1, 100, 91, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 92, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 93, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 94, 0, 0, 0)

// 	testCfg.MinCapacityUsedRatio = 0.3
// 	testCfg.MaxCapacityUsedRatio = 0.9

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Now the region is (1,3,4), the balance operators should be
// 	// 1) add peer: 2
// 	// 2) remove peer: 4
// 	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 60, 0, 0, 0)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 2)

// 	op1 := bop.Ops[0].(*changePeerOperator)
// 	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

// 	op2 := bop.Ops[1].(*changePeerOperator)
// 	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

// 	// If the sending snapshot count of `from store` is greater than MaxSnapSendingCount,
// 	// we will do nothing.
// 	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 65, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 60, 10, 0, 0)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, NotNil)

// 	op1 = bop.Ops[0].(*changePeerOperator)
// 	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

// 	op2 = bop.Ops[1].(*changePeerOperator)
// 	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(3))

// 	// If the receiving snapshot count of `to store` is greater than MaxReceivingSnapCount,
// 	// we will do nothing.
// 	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 10, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 65, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 60, 10, 0, 0)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// If the applying snapshot count of `to store` is greater than MaxApplyingSnapCount,
// 	// we will do nothing.
// 	s.updateStore(c, clusterInfo, 1, 100, 90, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 80, 0, 0, 10)
// 	s.updateStore(c, clusterInfo, 3, 100, 65, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 60, 10, 0, 0)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// If cluster max peer count config is 1, we can only add peer and remove leader peer.
// 	s.updateStore(c, clusterInfo, 1, 100, 60, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0, 0)

// 	// Set cluster config.
// 	oldMeta := clusterInfo.getMeta()
// 	meta := &metapb.Cluster{
// 		Id:           0,
// 		MaxPeerCount: 1,
// 	}
// 	clusterInfo.putMeta(meta)

// 	testCfg.MinCapacityUsedRatio = 0.3
// 	testCfg.MaxCapacityUsedRatio = 0.9
// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Set region peers to one peer.
// 	peers := region.GetPeers()
// 	region.Peers = []*metapb.Peer{leader}
// 	clusterInfo.putRegion(region)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, NotNil)

// 	op1 = bop.Ops[0].(*changePeerOperator)
// 	c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

// 	op2 = bop.Ops[1].(*changePeerOperator)
// 	c.Assert(op2.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op2.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(1))

// 	// If the diff score is too small, we should do nothing.
// 	s.updateStore(c, clusterInfo, 1, 100, 85, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0, 0)

// 	cb = newCapacityBalancer(testCfg)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Reset cluster config and region peers.
// 	clusterInfo.putMeta(oldMeta)

// 	region.Peers = peers
// 	clusterInfo.putRegion(region)
// }

// // TODO: Refactor these tests, they are quite ugly now.
// func (s *testBalancerSuite) TestDownStore(c *C) {
// 	clusterInfo := s.newClusterInfo(c)
// 	c.Assert(clusterInfo, NotNil)

// 	region := clusterInfo.searchRegion([]byte("a"))
// 	c.Assert(region.GetPeers(), HasLen, 1)

// 	testCfg := newBalanceConfig()
// 	testCfg.adjust()
// 	testCfg.MinCapacityUsedRatio = 0.1
// 	testCfg.MaxCapacityUsedRatio = 0.9
// 	testCfg.MaxStoreDownDuration.Duration = 1 * time.Second
// 	cb := newCapacityBalancer(testCfg)

// 	// The store id will be 1,2,3,4.
// 	s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 50, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 70, 0, 0, 0)

// 	// Test add peer.
// 	s.addRegionPeer(c, clusterInfo, 4, region)

// 	// Test add another peer.
// 	s.addRegionPeer(c, clusterInfo, 3, region)

// 	// Make store 2 up and down.
// 	for i := 0; i < 3; i++ {
// 		// Now we should move from store 4 to 2.
// 		s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0, 0)
// 		s.updateStore(c, clusterInfo, 2, 100, 70, 0, 0, 0)
// 		s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0, 0)
// 		s.updateStore(c, clusterInfo, 4, 100, 50, 0, 0, 0)

// 		_, bop, err := cb.Balance(clusterInfo)
// 		c.Assert(err, IsNil)
// 		c.Assert(bop.Ops, HasLen, 2)

// 		op0 := bop.Ops[0].(*changePeerOperator)
// 		c.Assert(op0.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 		c.Assert(op0.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

// 		op1 := bop.Ops[1].(*changePeerOperator)
// 		c.Assert(op1.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 		c.Assert(op1.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))

// 		// Update store 1,3,4 and let store 2 down.
// 		time.Sleep(600 * time.Millisecond)
// 		s.updateStore(c, clusterInfo, 1, 100, 80, 0, 0, 0)
// 		s.updateStore(c, clusterInfo, 3, 100, 60, 0, 0, 0)
// 		s.updateStore(c, clusterInfo, 4, 100, 50, 0, 0, 0)
// 		time.Sleep(600 * time.Millisecond)

// 		// Now store 2 is down, we should not do balance.
// 		_, bop, err = cb.Balance(clusterInfo)
// 		c.Assert(err, IsNil)
// 		c.Assert(bop, IsNil)
// 	}
// }

// func (s *testBalancerSuite) TestReplicaBalancer(c *C) {
// 	clusterInfo := s.newClusterInfo(c)
// 	c.Assert(clusterInfo, NotNil)

// 	region := clusterInfo.searchRegion([]byte("a"))
// 	c.Assert(region.GetPeers(), HasLen, 1)

// 	// The store id will be 1,2,3,4.
// 	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0, 0)

// 	// Test add peer.
// 	s.addRegionPeer(c, clusterInfo, 4, region)

// 	// Test add another peer.
// 	s.addRegionPeer(c, clusterInfo, 3, region)

// 	// Now peers count equals to max peer count, so there is nothing to do.
// 	db := newReplicaBalancer(region, s.cfg)
// 	_, bop, err := db.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Mock add one more peer.
// 	id, err := clusterInfo.allocID()
// 	c.Assert(err, IsNil)

// 	newPeer := s.newPeer(c, uint64(2), id)
// 	region.Peers = append(region.Peers, newPeer)

// 	// Test remove peer.
// 	db = newReplicaBalancer(region, s.cfg)
// 	_, bop, err = db.Balance(clusterInfo)
// 	c.Assert(err, IsNil)

// 	// Now we cannot remove leader peer, so the result is peer in store 2.
// 	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))
// }

// func (s *testBalancerSuite) TestReplicaBalancerWithDownPeers(c *C) {
// 	clusterInfo := s.newClusterInfo(c)
// 	c.Assert(clusterInfo, NotNil)

// 	region := clusterInfo.searchRegion([]byte("a"))
// 	c.Assert(region.GetPeers(), HasLen, 1)

// 	// The store id will be 1,2,3,4.
// 	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 40, 0, 0, 0)

// 	// Test add peer.
// 	s.addRegionPeer(c, clusterInfo, 4, region)

// 	// Test add another peer.
// 	s.addRegionPeer(c, clusterInfo, 3, region)

// 	region.DownPeers = []*pdpb.PeerStats{
// 		{
// 			Peer:        region.GetPeers()[1],
// 			DownSeconds: new(uint64),
// 		},
// 	}

// 	// DownSeconds < s.cfg.MaxPeerDownDuration, so there is nothing to do.
// 	*region.DownPeers[0].DownSeconds = 100
// 	rb := newReplicaBalancer(region, s.cfg)
// 	_, bop, err := rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Make store 4 down.
// 	s.cfg.MaxStoreDownDuration.Duration = 1 * time.Second
// 	time.Sleep(600 * time.Millisecond)
// 	s.updateStore(c, clusterInfo, 1, 100, 10, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 20, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 30, 0, 0, 0)
// 	time.Sleep(600 * time.Millisecond)

// 	// DownSeconds < s.cfg.MaxPeerDownDuration, but store 4 is down, add replica to store 2.
// 	rb = newReplicaBalancer(region, s.cfg)
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 1)

// 	op, ok := bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

// 	// DownSeconds >= s.cfg.MaxPeerDownDuration, add a replica to store 2.
// 	*region.DownPeers[0].DownSeconds = 60 * 60
// 	rb = newReplicaBalancer(region, s.cfg)
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 1)

// 	op, ok = bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(2))

// 	// Now we have enough active replicas, we can remove the down peer in store 4.
// 	addRegionPeer(c, region.Region, op.ChangePeer.GetPeer())
// 	clusterInfo.putRegion(region)

// 	rb = newReplicaBalancer(region, s.cfg)
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 1)

// 	op, ok = bop.Ops[0].(*onceOperator).Op.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, uint64(4))
// }

// func (s *testBalancerSuite) testReplicaBalancerWithNonUpState(c *C, state metapb.StoreState) {
// 	clusterInfo := s.newClusterInfo(c)
// 	c.Assert(clusterInfo, NotNil)

// 	region := clusterInfo.searchRegion([]byte("a"))
// 	c.Assert(region.GetPeers(), HasLen, 1)

// 	cfg := newBalanceConfig()
// 	cfg.adjust()
// 	cfg.MinCapacityUsedRatio = 0
// 	cb := newCapacityBalancer(cfg)
// 	rb := newReplicaBalancer(region, cfg)

// 	// The store id will be 1,2,3,4.
// 	s.updateStore(c, clusterInfo, 1, 100, 40, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 2, 100, 30, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 3, 100, 20, 0, 0, 0)
// 	s.updateStore(c, clusterInfo, 4, 100, 10, 0, 0, 0)

// 	s.addRegionPeer(c, clusterInfo, 2, region)
// 	s.addRegionPeer(c, clusterInfo, 3, region)

// 	// Transfer peer to store 4 if it is up.
// 	s.updateStore(c, clusterInfo, 4, 100, 90, 0, 0, 0)
// 	_, bop, err := cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	checkTransferPeer(c, bop.Ops, 3, 4)

// 	// No balance if store 4 is not up.
// 	s.updateStoreState(c, clusterInfo, 4, state)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Do balance if store 4 is up.
// 	s.updateStoreState(c, clusterInfo, 4, metapb.StoreState_Up)
// 	_, bop, err = cb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	checkTransferPeer(c, bop.Ops, 3, 4)

// 	// No balance if store 2 is up.
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop, IsNil)

// 	// Do balance if store 2 is not up.
// 	s.updateStoreState(c, clusterInfo, 2, state)
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 1)
// 	checkOnceAddPeer(c, bop.Ops[0], 4)

// 	s.addRegionPeer(c, clusterInfo, 4, region)
// 	_, bop, err = rb.Balance(clusterInfo)
// 	c.Assert(err, IsNil)
// 	c.Assert(bop.Ops, HasLen, 1)
// 	checkOnceRemovePeer(c, bop.Ops[0], 2)
// }

// func (s *testBalancerSuite) TestReplicaBalancerWithOffline(c *C) {
// 	s.testReplicaBalancerWithNonUpState(c, metapb.StoreState_Offline)
// }

// func (s *testBalancerSuite) TestReplicaBalancerWithTombstone(c *C) {
// 	s.testReplicaBalancerWithNonUpState(c, metapb.StoreState_Tombstone)
// }

// func checkAddPeer(c *C, oper Operator, addID uint64) {
// 	op, ok := oper.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_AddNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, addID)
// }

// func checkOnceAddPeer(c *C, oper Operator, addID uint64) {
// 	checkAddPeer(c, oper.(*onceOperator).Op, addID)
// }

// func checkRemovePeer(c *C, oper Operator, removeID uint64) {
// 	op, ok := oper.(*changePeerOperator)
// 	c.Assert(ok, IsTrue)
// 	c.Assert(op.ChangePeer.GetChangeType(), Equals, raftpb.ConfChangeType_RemoveNode)
// 	c.Assert(op.ChangePeer.GetPeer().GetStoreId(), Equals, removeID)
// }

// func checkOnceRemovePeer(c *C, oper Operator, removeID uint64) {
// 	checkRemovePeer(c, oper.(*onceOperator).Op, removeID)
// }

// func checkTransferPeer(c *C, ops []Operator, removeID uint64, addID uint64) {
// 	c.Assert(ops, HasLen, 2)
// 	checkAddPeer(c, ops[0], addID)
// 	checkRemovePeer(c, ops[1], removeID)
// }
