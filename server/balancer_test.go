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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ngaut/log"
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

func (c *testClusterInfo) addLabelsStore(storeID uint64, regionCount int, storageRatio float64, labels map[string]string) {
	c.addRegionStore(storeID, regionCount, storageRatio)
	store := c.getStore(storeID)
	for k, v := range labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: k, Value: v})
	}
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
	store.stats.TotalRegionCount = regionCount
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
	cfg := NewConfig()
	cfg.adjust()
	opt := newScheduleOption(cfg)
	return &cfg.ScheduleCfg, opt
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

	// Add stores 1,2,3,4
	tc.addLeaderStore(1, 6, 30)
	tc.addLeaderStore(2, 7, 30)
	tc.addLeaderStore(3, 8, 30)
	tc.addLeaderStore(4, 9, 30)
	// Add region 1 with leader in store 4 and followers in stores 1,2,3.
	tc.addLeaderRegion(1, 4, 1, 2, 3)

	// Test leaderCountFilter.
	// When leaderCount < 10, no schedule.
	c.Assert(lb.Schedule(cluster), IsNil)
	tc.updateLeaderCount(4, 11, 30)
	// When leaderCount > 10, transfer leader
	// from store 4 (with most leaders) to store 1 (with least leaders).
	checkTransferLeader(c, lb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	// If store 1 is down, it will be filtered,
	// store 2 becomes the store with least leaders.
	tc.setStoreDown(1)
	checkTransferLeader(c, lb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	// When diff leader ratio < MinBalanceDiffRatio, no schedule.
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

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 6, 0.1)
	tc.addRegionStore(2, 7, 0.2)
	tc.addRegionStore(3, 8, 0.3)
	tc.addRegionStore(4, 9, 0.4)
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)

	// Test regionCountFilter.
	// When regionCount < 10, no schedule.
	c.Assert(sb.Schedule(cluster), IsNil)
	tc.updateRegionCount(4, 11, 0.4)
	// When regionCount > 11, transfer peer
	// from store 4 (with most regions) to store 1 (with least regions).
	checkTransferPeer(c, sb.Schedule(cluster), 4, 1)

	// Test stateFilter.
	tc.setStoreOffline(1)
	// When store 1 is offline, it will be filtered,
	// store 2 becomes the store with least regions.
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)

	// Test MinBalanceDiffRatio.
	// When diff storage ratio < MinBalanceDiffRatio, no schedule.
	tc.updateRegionCount(2, 6, 0.4)
	tc.updateRegionCount(3, 7, 0.4)
	tc.updateRegionCount(4, 8, 0.4)
	c.Assert(sb.Schedule(cluster), IsNil)
}

func (s *testStorageBalancerSuite) TestConstraints(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	cfg.MinRegionCount = 1
	cfg.MinBalanceDiffRatio = 0.01
	opt.constraints, _ = newConstraints(1, []*Constraint{
		{
			Labels:   map[string]string{"zone": "cn"},
			Replicas: 1,
		},
	})
	sb := newStorageBalancer(opt)

	// Add stores 1,2,3,4.
	tc.addLabelsStore(1, 6, 0.1, nil)
	tc.addLabelsStore(2, 7, 0.2, nil)
	tc.addLabelsStore(3, 8, 0.3, nil)
	tc.addLabelsStore(4, 9, 0.4, map[string]string{"zone": "cn", "disk": "ssd"})
	// Add region 1 with leader in store 4.
	tc.addLeaderRegion(1, 4)

	// Store 4 has most regions, but no other stores can match the constraint.
	c.Assert(sb.Schedule(cluster), IsNil)

	// Now store 3 can match the constarint too,
	// We can transfer peer from store 4 to store 3.
	tc.addLabelsStore(3, 8, 0.3, map[string]string{"zone": "cn"})
	checkTransferPeer(c, sb.Schedule(cluster), 4, 3)

	// Now both store 2 and 3 can match the constraint.
	// But store 2 has fewer regions than store 3, so transfer peer to store 2.
	tc.addLabelsStore(2, 7, 0.2, map[string]string{"zone": "cn", "disk": "hdd"})
	checkTransferPeer(c, sb.Schedule(cluster), 4, 2)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct{}

func (s *testReplicaCheckerSuite) Test(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	rc := newReplicaChecker(cluster, opt)

	cfg.MaxSnapshotCount = 2

	// Add stores 1,2,3,4.
	tc.addRegionStore(1, 4, 0.4)
	tc.addRegionStore(2, 3, 0.3)
	tc.addRegionStore(3, 2, 0.1)
	tc.addRegionStore(4, 1, 0.2)
	// Add region 1 with leader in store 1 and follower in store 2.
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
	// If store 4 is down, we have no other stores to add peer.
	tc.setStoreDown(4)
	c.Assert(rc.Check(region), IsNil)

	// Test snapshotCountFilter.
	// If snapshotCount < MaxSnapshotCount, we can add peer.
	tc.setStoreUp(4)
	checkAddPeer(c, rc.Check(region), 4)
	// If snapshotCount > MaxSnapshotCount, we can not add peer.
	tc.updateSnapshotCount(4, 3)
	c.Assert(rc.Check(region), IsNil)
	tc.updateSnapshotCount(4, 1)
	checkAddPeer(c, rc.Check(region), 4)

	// Remove redundant peer.
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 1)
}

func (s *testReplicaCheckerSuite) TestConstraints(c *C) {
	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	cfg, opt := newTestScheduleConfig()
	cfg.MinRegionCount = 1
	cfg.MinBalanceDiffRatio = 0.01
	opt.constraints, _ = newConstraints(3, []*Constraint{
		{
			Labels:   map[string]string{"zone": "us"},
			Replicas: 2,
		},
		{
			Labels:   map[string]string{"zone": "cn", "disk": "ssd"},
			Replicas: 1,
		},
	})
	rc := newReplicaChecker(cluster, opt)

	// Add stores 1,2,3,4.
	tc.addLabelsStore(1, 6, 0.1, map[string]string{"zone": "us", "disk": "ssd"})
	tc.addLabelsStore(2, 7, 0.2, nil)
	tc.addLabelsStore(3, 8, 0.3, nil)
	tc.addLabelsStore(4, 9, 0.4, nil)
	// Add region 1 with leader in store 1.
	tc.addLeaderRegion(1, 1)

	// Although we can not satisfy any constraints, we will still ensure max replicas.
	// So we add peers in store 2 and store 3, because they have smaller storage ratio.
	region := cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 2)
	peer2, _ := cluster.allocPeer(2)
	region.Peers = append(region.Peers, peer2)
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ := cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	log.Debug(region)
	c.Assert(rc.Check(region), IsNil)

	// Add stores 1,2,3,4.
	tc.addLabelsStore(1, 6, 0.1, map[string]string{"zone": "us", "disk": "ssd"})
	tc.addLabelsStore(2, 7, 0.2, map[string]string{"zone": "us", "disk": "hdd"})
	tc.addLabelsStore(3, 8, 0.3, nil)
	tc.addLabelsStore(4, 9, 0.4, nil)
	// Add region 1 with leader in store 1 and follower in store 2.
	tc.addLeaderRegion(1, 1, 2)

	// Now the first constraint has been satisfied, but the second hasn't.
	// We still need to add peer in store 3 to ensure max replicas.
	region = cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 3)
	peer3, _ = cluster.allocPeer(3)
	region.Peers = append(region.Peers, peer3)
	c.Assert(rc.Check(region), IsNil)

	// Add stores 1,2,3,4.
	tc.addLabelsStore(1, 6, 0.1, map[string]string{"zone": "us", "disk": "ssd"})
	tc.addLabelsStore(2, 7, 0.2, map[string]string{"zone": "us", "disk": "hdd"})
	tc.addLabelsStore(3, 8, 0.3, nil)
	tc.addLabelsStore(4, 9, 0.4, map[string]string{"zone": "cn", "disk": "ssd"})
	// Add region 1 with leader in store 1 and followers in stores 2,3.
	tc.addLeaderRegion(1, 1, 2, 3)

	// Stores 1,2 satisfy the first constraint, we need to add peer in
	// store 4 to satisfy the second constraint.
	// Then we can remove peer in store 3 because we have satisfied
	// all constraints and have enough replicas.
	region = cluster.getRegion(1)
	checkAddPeer(c, rc.Check(region), 4)
	peer4, _ := cluster.allocPeer(4)
	region.Peers = append(region.Peers, peer4)
	checkRemovePeer(c, rc.Check(region), 3)
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
