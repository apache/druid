/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver;
import org.apache.druid.jackson.StrictTypeIdResolver;

import javax.annotation.Nullable;

/**
 * Streaming analog of the batch/compaction {@code partitionsSpec}. Configured in the streaming tuning config, it
 * declares <em>what</em> a streaming task should collect from its rows so each published segment can be stamped with a
 * prunable {@link org.apache.druid.timeline.partition.ShardSpec}, letting the broker prune segments at query time
 * without waiting for compaction. Unlike batch partitioning this does not route rows into shards; it only annotates
 * segments based on the values they happened to ingest.
 *
 * <p>This is a polymorphic, pluggable type (mirroring {@link org.apache.druid.indexer.partitions.PartitionsSpec}): each
 * implementation pairs a per-task {@link StreamingShardSpecCollector} (the "collect something per row, then build a
 * shard spec" operation) with the shard-spec type it produces. The built-in {@link DimensionValueSetPartitionsSpec}
 * collects the distinct values of a set of dimensions; future strategies (e.g. min/max ranges or bloom filters) can be
 * added by registering a new subtype below with its own collector, without changing the task runner.
 *
 * <p>An omitted {@code type} defaults to {@link DimensionValueSetPartitionsSpec} for backward compatibility, but an
 * explicit unknown {@code type} (a typo, or a subtype whose extension isn't loaded here) is rejected by
 * {@link StrictTypeIdResolver} rather than silently falling back.
 */
@JsonTypeResolver(StrictTypeIdResolver.Builder.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", defaultImpl = DimensionValueSetPartitionsSpec.class)
@JsonSubTypes({
    @JsonSubTypes.Type(name = DimensionValueSetPartitionsSpec.TYPE, value = DimensionValueSetPartitionsSpec.class)
})
public interface StreamingPartitionsSpec
{
  /**
   * Creates a task-scoped {@link StreamingShardSpecCollector} that accumulates per-row information and stamps each
   * segment's shard spec with it at publish time. Returns {@code null} when this spec is configured such that there is
   * nothing to collect (e.g. no dimensions), in which case no collector runs and segments are published unchanged.
   * One collector is created per task run.
   */
  @Nullable
  StreamingShardSpecCollector createCollector();
}
