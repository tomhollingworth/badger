/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"expvar"
	"fmt"
)

const (
	BADGER_METRIC_PREFIX = "badger_"
)

var (
	// lsmSize has size of the LSM in bytes
	lsmSize *expvar.Map
	// vlogSize has size of the value log in bytes
	vlogSize *expvar.Map
	// pendingWrites tracks the number of pending writes.
	pendingWrites *expvar.Map

	// These are cumulative

	// VLOG METRICS
	// numReads has cumulative number of reads from vlog
	numReadsVlog *expvar.Int
	// numWrites has cumulative number of writes into vlog
	numWritesVlog *expvar.Int
	// numBytesRead has cumulative number of bytes read from VLOG
	numBytesReadVlog *expvar.Int
	// numBytesVlogWritten has cumulative number of bytes written into VLOG
	numBytesVlogWritten *expvar.Int

	// LSM METRICS
	// numBytesRead has cumulative number of bytes read from LSM tree
	numBytesReadLSM *expvar.Int
	// numBytesWrittenToL0 has cumulative number of bytes written into LSM Tree
	numBytesWrittenToL0 *expvar.Int
	// numLSMGets is number of LSM gets
	numLSMGets *expvar.Map
	// numBytesCompactionWritten is the number of bytes written in the lsm tree due to compaction
	numBytesCompactionWritten *expvar.Map
	// numLSMBloomHits is number of LMS bloom hits
	numLSMBloomHits *expvar.Map

	// DB METRICS
	// numGets is number of gets -> Number of get requests made
	numGets *expvar.Int
	// number of get queries in which we actually get a result
	numGetsWithResults *expvar.Int
	// number of iterators created, these would be the number of range queries
	numIteratorsCreated *expvar.Int
	// numPuts is number of puts -> Number of puts requests made
	numPuts *expvar.Int
	// numMemtableGets is number of memtable gets -> Number of get requests made on memtable
	numMemtableGets *expvar.Int
	// numCompactionTables is the number of tables being compacted
	numCompactionTables *expvar.Int
	// numCompactions is the total number of compactions that have occurred
	numCompactions *expvar.Int
	// numCompactionsByLevel tracks compactions by level transition (e.g., "L0->L1")
	numCompactionsByLevel *expvar.Map
	// compactionDurationOverall tracks compaction duration as a histogram (overall)
	compactionDurationOverall *histogram
	// compactionDurationByLevel tracks compaction duration as a histogram by level
	compactionDurationByLevel *expvar.Map
	// Total writes by a user in bytes
	numBytesWrittenUser *expvar.Int
)

// These variables are global and have cumulative values for all kv stores.
// Naming convention of metrics: {badger_version}_{singular operation}_{granularity}_{component}
func init() {
	numReadsVlog = expvar.NewInt(BADGER_METRIC_PREFIX + "read_num_vlog")
	numBytesReadVlog = expvar.NewInt(BADGER_METRIC_PREFIX + "read_bytes_vlog")
	numWritesVlog = expvar.NewInt(BADGER_METRIC_PREFIX + "write_num_vlog")
	numBytesVlogWritten = expvar.NewInt(BADGER_METRIC_PREFIX + "write_bytes_vlog")

	numBytesReadLSM = expvar.NewInt(BADGER_METRIC_PREFIX + "read_bytes_lsm")
	numBytesWrittenToL0 = expvar.NewInt(BADGER_METRIC_PREFIX + "write_bytes_l0")
	numBytesCompactionWritten = expvar.NewMap(BADGER_METRIC_PREFIX + "write_bytes_compaction")

	numLSMGets = expvar.NewMap(BADGER_METRIC_PREFIX + "get_num_lsm")
	numLSMBloomHits = expvar.NewMap(BADGER_METRIC_PREFIX + "hit_num_lsm_bloom_filter")
	numMemtableGets = expvar.NewInt(BADGER_METRIC_PREFIX + "get_num_memtable")

	// User operations
	numGets = expvar.NewInt(BADGER_METRIC_PREFIX + "get_num_user")
	numPuts = expvar.NewInt(BADGER_METRIC_PREFIX + "put_num_user")
	numBytesWrittenUser = expvar.NewInt(BADGER_METRIC_PREFIX + "write_bytes_user")

	// Required for Enabled
	numGetsWithResults = expvar.NewInt(BADGER_METRIC_PREFIX + "get_with_result_num_user")
	numIteratorsCreated = expvar.NewInt(BADGER_METRIC_PREFIX + "iterator_num_user")

	// Sizes
	lsmSize = expvar.NewMap(BADGER_METRIC_PREFIX + "size_bytes_lsm")
	vlogSize = expvar.NewMap(BADGER_METRIC_PREFIX + "size_bytes_vlog")

	pendingWrites = expvar.NewMap(BADGER_METRIC_PREFIX + "write_pending_num_memtable")
	numCompactionTables = expvar.NewInt(BADGER_METRIC_PREFIX + "compaction_current_num_lsm")

	// Compaction metrics
	numCompactions = expvar.NewInt(BADGER_METRIC_PREFIX + "compaction_num_total")
	numCompactionsByLevel = expvar.NewMap(BADGER_METRIC_PREFIX + "compaction_num_by_level")

	// Compaction duration histograms
	// Buckets: 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s, 30s, 60s, more
	compactionDurationOverall = newHistogram(
		BADGER_METRIC_PREFIX+"compaction_duration_seconds",
		[]float64{0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0, 5.0, 10.0, 30.0, 60.0})
	compactionDurationByLevel = expvar.NewMap(BADGER_METRIC_PREFIX + "compaction_duration_seconds_by_level")
}

func NumIteratorsCreatedAdd(enabled bool, val int64) {
	addInt(enabled, numIteratorsCreated, val)
}

func NumGetsWithResultsAdd(enabled bool, val int64) {
	addInt(enabled, numGetsWithResults, val)
}

func NumReadsVlogAdd(enabled bool, val int64) {
	addInt(enabled, numReadsVlog, val)
}

func NumBytesWrittenUserAdd(enabled bool, val int64) {
	addInt(enabled, numBytesWrittenUser, val)
}

func NumWritesVlogAdd(enabled bool, val int64) {
	addInt(enabled, numWritesVlog, val)
}

func NumBytesReadsVlogAdd(enabled bool, val int64) {
	addInt(enabled, numBytesReadVlog, val)
}

func NumBytesReadsLSMAdd(enabled bool, val int64) {
	addInt(enabled, numBytesReadLSM, val)
}

func NumBytesWrittenVlogAdd(enabled bool, val int64) {
	addInt(enabled, numBytesVlogWritten, val)
}

func NumBytesWrittenToL0Add(enabled bool, val int64) {
	addInt(enabled, numBytesWrittenToL0, val)
}

func NumBytesCompactionWrittenAdd(enabled bool, key string, val int64) {
	addToMap(enabled, numBytesCompactionWritten, key, val)
}

func NumGetsAdd(enabled bool, val int64) {
	addInt(enabled, numGets, val)
}

func NumPutsAdd(enabled bool, val int64) {
	addInt(enabled, numPuts, val)
}

func NumMemtableGetsAdd(enabled bool, val int64) {
	addInt(enabled, numMemtableGets, val)
}

func NumCompactionTablesAdd(enabled bool, val int64) {
	addInt(enabled, numCompactionTables, val)
}

func NumCompactionsAdd(enabled bool, val int64) {
	addInt(enabled, numCompactions, val)
}

// histogramBucket represents a single bucket in the histogram
type histogramBucket struct {
	upperBound float64
	count      *expvar.Int
}

// histogram represents a histogram with multiple buckets
type histogram struct {
	name       string
	buckets    []*histogramBucket
	infCount   *expvar.Int // Count for values > max bucket upper bound
	totalCount *expvar.Int
	sum        *expvar.Float
}

// newHistogram creates a new histogram with the given bucket bounds (in seconds)
func newHistogram(name string, buckets []float64) *histogram {
	h := &histogram{
		name:       name,
		buckets:    make([]*histogramBucket, len(buckets)),
		infCount:   expvar.NewInt(name + "_count_inf"),
		totalCount: expvar.NewInt(name + "_count"),
		sum:        expvar.NewFloat(name + "_sum"),
	}
	for i, bound := range buckets {
		h.buckets[i] = &histogramBucket{
			upperBound: bound,
			count:      expvar.NewInt(name + "_bucket{le=\"" + fmt.Sprintf("%g", bound) + "\"}"),
		}
	}
	return h
}

// observe adds a duration to the histogram (duration should be in seconds)
func (h *histogram) observe(d float64) {
	h.sum.Add(d)
	h.totalCount.Add(1)
	for _, b := range h.buckets {
		if d <= b.upperBound {
			b.count.Add(1)
			return
		}
	}
	// Value is above all buckets, increment the infinity bucket
	h.infCount.Add(1)
}

// String returns a string representation of the histogram (required for expvar.Var interface)
func (h *histogram) String() string {
	return fmt.Sprintf("%s: totalCount=%d, sum=%.6f", h.name, h.totalCount.Value(), h.sum.Value())
}

func NumCompactionsByLevelAdd(enabled bool, fromLevel, toLevel int, val int64) {
	if !enabled {
		return
	}
	key := fmt.Sprintf("L%d->L%d", fromLevel, toLevel)
	addToMap(enabled, numCompactionsByLevel, key, val)
}

// CompactionDurationObserve records a compaction duration in the overall histogram
func CompactionDurationObserve(enabled bool, duration float64) {
	if !enabled || compactionDurationOverall == nil {
		return
	}
	compactionDurationOverall.observe(duration)
}

// CompactionDurationByLevelObserve records a compaction duration for a specific level
func CompactionDurationByLevelObserve(enabled bool, level int, duration float64) {
	if !enabled {
		return
	}
	key := fmt.Sprintf("L%d", level)
	if h, ok := compactionDurationByLevel.Get(key).(*histogram); ok {
		h.observe(duration)
		return
	}
	// Create new histogram for this level if it doesn't exist
	h := newHistogram(BADGER_METRIC_PREFIX+"compaction_duration_seconds_by_level{level=\""+key+"\"}",
		[]float64{0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0, 5.0, 10.0, 30.0, 60.0})
	compactionDurationByLevel.Set(key, h)
	h.observe(duration)
}

func LSMSizeSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, lsmSize, key, val)
}

func VlogSizeSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, vlogSize, key, val)
}

func PendingWritesSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, pendingWrites, key, val)
}

func NumLSMBloomHitsAdd(enabled bool, key string, val int64) {
	addToMap(enabled, numLSMBloomHits, key, val)
}

func NumLSMGetsAdd(enabled bool, key string, val int64) {
	addToMap(enabled, numLSMGets, key, val)
}

func LSMSizeGet(enabled bool, key string) expvar.Var {
	return getFromMap(enabled, lsmSize, key)
}

func VlogSizeGet(enabled bool, key string) expvar.Var {
	return getFromMap(enabled, vlogSize, key)
}

func addInt(enabled bool, metric *expvar.Int, val int64) {
	if !enabled {
		return
	}

	metric.Add(val)
}

func addToMap(enabled bool, metric *expvar.Map, key string, val int64) {
	if !enabled {
		return
	}

	metric.Add(key, val)
}

func storeToMap(enabled bool, metric *expvar.Map, key string, val expvar.Var) {
	if !enabled {
		return
	}

	metric.Set(key, val)
}

func getFromMap(enabled bool, metric *expvar.Map, key string) expvar.Var {
	if !enabled {
		return nil
	}

	return metric.Get(key)
}
