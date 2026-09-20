/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"

	. "github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

func BenchmarkRedisClear(b *testing.B) {
	for _, unrelated := range []int{0, 32768} {
		b.Run(fmt.Sprintf("target=2048/unrelated=%d/pods=8", unrelated), func(b *testing.B) {
			b.StopTimer()
			index, ctx := redisBenchmarkIndex(b)
			keys, pods := redisBenchmarkEntries()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				require.NoError(b, index.RedisClient.FlushDB(ctx).Err())
				for start := 0; start < len(keys); start += 512 {
					require.NoError(b, index.Add(ctx, nil, keys[start:start+512], pods))
				}
				pipe := index.RedisClient.Pipeline()
				for j := 0; j < unrelated; j++ {
					pipe.HSet(ctx, fmt.Sprintf("unrelated:%d", j), `{"podIdentifier":"other"}`, "")
				}
				_, err := pipe.Exec(ctx)
				require.NoError(b, err)
				b.StartTimer()
				err = index.Clear(ctx, pods[0].PodIdentifier)
				b.StopTimer()
				require.NoError(b, err)
				hits, err := index.Lookup(ctx, keys, nil)
				require.NoError(b, err)
				require.Len(b, hits, len(keys))
				for _, key := range keys {
					require.ElementsMatch(b, pods[1:], hits[key])
				}
			}
		})
	}
}

func BenchmarkRedisAdd(b *testing.B) {
	b.StopTimer()
	index, ctx := redisBenchmarkIndex(b)
	keys, pods := redisBenchmarkEntries()
	var datasetBytes int64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		require.NoError(b, index.RedisClient.FlushDB(ctx).Err())
		before := redisBenchmarkMemory(b, ctx, index)
		b.StartTimer()
		err := index.Add(ctx, nil, keys, pods)
		b.StopTimer()
		require.NoError(b, err)
		datasetBytes += redisBenchmarkMemory(b, ctx, index) - before
	}
	if os.Getenv("KV_CACHE_BENCH_REDIS_ADDR") != "" {
		b.ReportMetric(float64(datasetBytes)/float64(b.N), "redis-bytes/op")
	}
}

func redisBenchmarkIndex(b *testing.B) (*RedisIndex, context.Context) {
	b.Helper()
	address := os.Getenv("KV_CACHE_BENCH_REDIS_ADDR")
	if address == "" {
		address = miniredis.RunT(b).Addr()
	}
	index, err := NewRedisIndex(&RedisIndexConfig{Address: address})
	require.NoError(b, err)
	redisIndex, ok := index.(*RedisIndex)
	require.True(b, ok)
	ctx := log.IntoContext(b.Context(), logr.Discard())
	b.Cleanup(func() {
		ctx := context.Background()
		require.NoError(b, redisIndex.RedisClient.FlushDB(ctx).Err())
		require.NoError(b, redisIndex.RedisClient.Close())
	})
	return redisIndex, ctx
}

func redisBenchmarkEntries() ([]BlockHash, []PodEntry) {
	keys := make([]BlockHash, 2048)
	for i := range keys {
		keys[i] = BlockHash(i + 1)
	}
	pods := make([]PodEntry, 8)
	for i := range pods {
		pods[i] = PodEntry{PodIdentifier: fmt.Sprintf("pod-%d", i), DeviceTier: "gpu"}
	}
	return keys, pods
}

func redisBenchmarkMemory(b *testing.B, ctx context.Context, index *RedisIndex) int64 {
	b.Helper()
	if os.Getenv("KV_CACHE_BENCH_REDIS_ADDR") == "" {
		return 0
	}
	info, err := index.RedisClient.Info(ctx, "memory").Result()
	require.NoError(b, err)
	for line := range strings.SplitSeq(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "used_memory:"); ok {
			bytes, err := strconv.ParseInt(value, 10, 64)
			require.NoError(b, err)
			return bytes
		}
	}
	b.Fatal("Redis INFO memory has no used_memory field")
	return 0
}
