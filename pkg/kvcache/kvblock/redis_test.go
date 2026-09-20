/*
Copyright 2025 The llm-d Authors.

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
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	. "github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// createRedisIndexForTesting creates a new RedisIndex with a mock Redis server for testing.
func createRedisIndexForTesting(t *testing.T) Index {
	t.Helper()
	return createRedisIndexForInspection(t)
}

func createRedisIndexForInspection(t testing.TB) *RedisIndex {
	t.Helper()
	server, err := miniredis.Run()
	require.NoError(t, err)

	// Store server reference for cleanup
	t.Cleanup(func() {
		server.Close()
	})

	redisConfig := &RedisIndexConfig{
		Address: server.Addr(),
	}
	index, err := NewRedisIndex(redisConfig)
	require.NoError(t, err)
	redisIndex, ok := index.(*RedisIndex)
	require.True(t, ok)
	t.Cleanup(func() {
		require.NoError(t, redisIndex.RedisClient.Close())
	})
	return redisIndex
}

// TestRedisIndexBehavior tests the Redis index implementation using common test behaviors.
func TestRedisIndexBehavior(t *testing.T) {
	testCommonIndexBehavior(t, createRedisIndexForTesting)
}

func TestRedisClearPreservesUnrelatedKeys(t *testing.T) {
	for _, keyType := range []string{"hash", "string"} {
		t.Run(keyType, func(t *testing.T) {
			index := createRedisIndexForInspection(t)
			ctx := t.Context()
			pod := PodEntry{PodIdentifier: "pod-clear", DeviceTier: "gpu"}
			key := BlockHash(1)
			field, err := json.Marshal(pod)
			require.NoError(t, err)
			require.NoError(t, index.Add(ctx, nil, []BlockHash{key}, []PodEntry{pod}))
			if keyType == "hash" {
				require.NoError(t, index.RedisClient.HSet(ctx, "unrelated", string(field), "keep").Err())
			} else {
				require.NoError(t, index.RedisClient.Set(ctx, "unrelated", "keep", 0).Err())
			}

			require.NoError(t, index.Clear(ctx, pod.PodIdentifier))

			if keyType == "hash" {
				value, err := index.RedisClient.HGet(ctx, "unrelated", string(field)).Result()
				require.NoError(t, err)
				require.Equal(t, "keep", value)
			} else {
				value, err := index.RedisClient.Get(ctx, "unrelated").Result()
				require.NoError(t, err)
				require.Equal(t, "keep", value)
			}
			exists, err := index.RedisClient.Exists(ctx, key.String()).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
		})
	}
}

func TestRedisEvictPrunesReverseIndex(t *testing.T) {
	for _, keyType := range []KeyType{EngineKey, RequestKey} {
		t.Run(map[KeyType]string{EngineKey: "engine", RequestKey: "request"}[keyType], func(t *testing.T) {
			index := createRedisIndexForInspection(t)
			ctx := t.Context()
			pod := PodEntry{PodIdentifier: "pod-evict", DeviceTier: "gpu"}
			otherTier := PodEntry{PodIdentifier: pod.PodIdentifier, DeviceTier: "cpu"}
			key := BlockHash(1)
			reverseKey := "kvblock:pod:" + pod.PodIdentifier + ":entries"
			for range 2 {
				require.NoError(t, index.Add(ctx, []BlockHash{key}, []BlockHash{key}, []PodEntry{pod, otherTier}))
			}
			require.EqualValues(t, 2, index.RedisClient.SCard(ctx, reverseKey).Val())

			require.NoError(t, index.Evict(ctx, key, keyType, []PodEntry{pod}))
			require.EqualValues(t, 1, index.RedisClient.SCard(ctx, reverseKey).Val())
			hits, err := index.Lookup(ctx, []BlockHash{key}, nil)
			require.NoError(t, err)
			require.Equal(t, []PodEntry{otherTier}, hits[key])

			require.NoError(t, index.Evict(ctx, key, keyType, []PodEntry{otherTier}))
			exists, err := index.RedisClient.Exists(ctx, key.String(), reverseKey).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
		})
	}
}

func TestRedisClearMultipleBatches(t *testing.T) {
	index := createRedisIndexForInspection(t)
	ctx := t.Context()
	pod := PodEntry{PodIdentifier: "pod-clear", DeviceTier: "gpu"}
	keys := make([]BlockHash, 3073)
	for i := range keys {
		keys[i] = BlockHash(i + 1)
	}
	require.NoError(t, index.Add(ctx, nil, keys, []PodEntry{pod}))
	require.NoError(t, index.Clear(ctx, pod.PodIdentifier))
	require.NoError(t, index.Clear(ctx, pod.PodIdentifier))
	remaining, err := index.RedisClient.Keys(ctx, "*").Result()
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestRedisClearDropsMalformedReverseEntries(t *testing.T) {
	index := createRedisIndexForInspection(t)
	ctx := t.Context()
	key := "kvblock:pod:pod-malformed:entries"
	require.NoError(t, index.RedisClient.SAdd(ctx, key, "malformed", "\x00{}", "1\x00").Err())
	require.NoError(t, index.Clear(ctx, "pod-malformed"))
	exists, err := index.RedisClient.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Zero(t, exists)
}

func TestRedisMutationsKeepReverseIndexConsistent(t *testing.T) {
	for _, operation := range []string{"add", "evict", "clear"} {
		t.Run(operation, func(t *testing.T) {
			index := createRedisIndexForInspection(t)
			ctx := t.Context()
			options := *index.RedisClient.Options()
			peer := &RedisIndex{RedisClient: redis.NewClient(&options)}
			t.Cleanup(func() { require.NoError(t, peer.RedisClient.Close()) })
			pod := PodEntry{PodIdentifier: "pod-concurrent", DeviceTier: "gpu"}
			key := BlockHash(1)
			require.NoError(t, index.Add(ctx, nil, []BlockHash{key}, []PodEntry{pod}))

			hook := &redisInterleaveHook{command: "hdel", interleave: func() error {
				return peer.Add(ctx, nil, []BlockHash{key}, []PodEntry{pod})
			}}
			if operation == "add" {
				hook.command = "hset"
				hook.interleave = func() error {
					return peer.Evict(ctx, key, RequestKey, []PodEntry{pod})
				}
			}
			index.RedisClient.AddHook(hook)
			switch operation {
			case "add":
				require.NoError(t, index.Add(ctx, nil, []BlockHash{key}, []PodEntry{pod}))
			case "evict":
				require.NoError(t, index.Evict(ctx, key, RequestKey, []PodEntry{pod}))
			case "clear":
				require.NoError(t, index.Clear(ctx, pod.PodIdentifier))
			}
			require.True(t, hook.called, "the second client's mutation must run")
			fields, err := index.RedisClient.HLen(ctx, key.String()).Result()
			require.NoError(t, err)
			members, err := index.RedisClient.SCard(ctx, "kvblock:pod:"+pod.PodIdentifier+":entries").Result()
			require.NoError(t, err)
			require.Equal(t, fields, members)
			require.NoError(t, peer.Clear(ctx, pod.PodIdentifier))
			exists, err := peer.RedisClient.Exists(ctx, key.String()).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
		})
	}
}

type redisInterleaveHook struct {
	command    string
	interleave func() error
	called     bool
}

func (h *redisInterleaveHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *redisInterleaveHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *redisInterleaveHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.called || len(cmds) == 0 {
			return next(ctx, cmds)
		}
		// Model another client's write between pipeline commands, preserving MULTI/EXEC.
		if cmds[0].Name() == "multi" {
			if err := next(ctx, cmds); err != nil {
				return err
			}
			h.called = true
			return h.interleave()
		}
		for _, cmd := range cmds {
			if err := next(ctx, []redis.Cmder{cmd}); err != nil {
				return err
			}
			if cmd.Name() == h.command && !h.called {
				h.called = true
				if err := h.interleave(); err != nil {
					return err
				}
			}
		}
		return nil
	}
}
