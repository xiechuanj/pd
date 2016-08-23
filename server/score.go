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
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const noThreshold = -1.0

type score struct {
	from      float64
	to        float64
	diff      float64
	threshold float64
	st        scoreType
}

func priorityScore(cfg *BalanceConfig, scores []*score) (int, *score) {
	var (
		maxPriority float64
		idx         int
		resultScore *score
	)

	for i, score := range scores {
		priority := score.diff
		if score.threshold != noThreshold {
			// If the from store score is close to threshold value, we should add the priority weight.
			if score.threshold-score.from <= cfg.MaxDiffScoreFraction {
				priority += 0.1
			}
		}

		if priority > maxPriority {
			idx = i
			resultScore = score
		}
	}

	return idx, resultScore
}

func scoreThreshold(cfg *BalanceConfig, st scoreType) float64 {
	switch st {
	case capacityScore:
		return cfg.MaxCapacityUsedRatio
	default:
		return noThreshold
	}
}

func calculateDiffScore(from, to uint64) float64 {
	return (float64(from) - float64(to)) / float64(from)
}

type scoreType byte

const (
	leaderScore scoreType = iota + 1
	capacityScore
)

func (st scoreType) String() string {
	switch st {
	case leaderScore:
		return "leader score"
	case capacityScore:
		return "capacity score"
	default:
		return "unknown"
	}
}

// Scorer is an interface to calculate the score.
type Scorer interface {
	// Score calculates the score of store.
	Score(store *storeInfo) float64
	// DiffScore calculates the diff score and post diff score of two stores.
	DiffScore(from *storeInfo, to *storeInfo) (float64, float64)
}

type leaderScorer struct {
}

func newLeaderScorer() *leaderScorer {
	return &leaderScorer{}
}

func (ls *leaderScorer) Score(store *storeInfo) float64 {
	return store.leaderRatio()
}

func (ls *leaderScorer) DiffScore(from *storeInfo, to *storeInfo) (float64, float64) {
	fromCount := uint64(from.stats.LeaderRegionCount)
	toCount := uint64(to.stats.LeaderRegionCount)
	if fromCount <= 1 || fromCount <= toCount {
		return 0.0, 0.0
	}
	diffScore := calculateDiffScore(fromCount, toCount)
	postDiffScore := calculateDiffScore(fromCount-1, toCount+1)
	return diffScore, postDiffScore
}

type capacityScorer struct {
}

func newCapacityScorer() *capacityScorer {
	return &capacityScorer{}
}

func (cs *capacityScorer) Score(store *storeInfo) float64 {
	return store.usedRatio()
}

func (cs *capacityScorer) DiffScore(from *storeInfo, to *storeInfo) (float64, float64) {
	// TODO: Get region size from store.
	regionSize := uint64(64 * 1024 * 1024)
	fromUsed := from.stats.Stats.GetCapacity() - from.stats.Stats.GetAvailable()
	toUsed := to.stats.Stats.GetCapacity() - to.stats.Stats.GetAvailable()
	if fromUsed <= regionSize || fromUsed <= toUsed {
		return 0.0, 0.0
	}
	diffScore := calculateDiffScore(fromUsed, toUsed)
	postDiffScore := calculateDiffScore(fromUsed-regionSize, toUsed+regionSize)
	return diffScore, postDiffScore
}

func newScorer(st scoreType) Scorer {
	switch st {
	case leaderScore:
		return newLeaderScorer()
	case capacityScore:
		return newCapacityScorer()
	}

	return nil
}

func checkAndGetDiffScore(cluster *clusterInfo, oldPeer *metapb.Peer, newPeer *metapb.Peer, st scoreType, cfg *BalanceConfig) (*score, bool) {
	oldStore := cluster.getStore(oldPeer.GetStoreId())
	newStore := cluster.getStore(newPeer.GetStoreId())
	if oldStore == nil || newStore == nil {
		log.Debugf("check score failed - old peer: %v, new peer: %v", oldPeer, newPeer)
		return nil, false
	}

	scorer := newScorer(st)
	oldStoreScore := scorer.Score(oldStore)
	newStoreScore := scorer.Score(newStore)
	diffScore, postDiffScore := scorer.DiffScore(oldStore, newStore)

	// Check whether the diff score is in MaxDiffScoreFraction range.
	if diffScore <= cfg.MaxDiffScoreFraction {
		log.Debugf("check score failed - diff score is too small - score type: %v, old peer: %v, new peer: %v, old store score: %v, new store score: %v, diif score: %v",
			st, oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore)
		return nil, false
	}
	if postDiffScore < 0 {
		log.Debugf("check score failed - post diff score is negative - score type: %v, old peer: %v, new peer: %v, old store score: %v, new store score: %v, diff score: %v, post diff score: %v",
			st, oldPeer, newPeer, oldStoreScore, newStoreScore, diffScore, postDiffScore)
		return nil, false
	}

	score := &score{
		from:      oldStoreScore,
		to:        newStoreScore,
		diff:      diffScore,
		threshold: scoreThreshold(cfg, st),
		st:        st,
	}

	return score, true
}
