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

package org.apache.druid.server.coordinator.rules;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.segment.loading.PartialClusterGroupLoadSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base for {@link PartialLoadMatcher} implementations that decide which of a clustered segment's cluster groups to
 * partially load. Subclasses supply the resolution policy via {@link #resolveClusterGroupIndices(DataSegment)}; the
 * sorted, deduped indices into {@code segment.getClusterGroups().getTuples()}, and this base handles fingerprint
 * computation and wraps the result into the {@code partialClusterGroup} load-spec wire form consumed by the
 * historical-side partial loader.
 * <p>
 * The fingerprint is a hash of the resolved indices for a segment; the data node includes this value in the segment
 * announcement so the coordinator can detect rule changes between runs and reconcile loaded replicas.
 */
public abstract class ClusterGroupPartialLoadMatcher implements PartialLoadMatcher
{
  static final String FINGERPRINT_VERSION = "v1";

  /**
   * Returns the sorted, deduped list of indices into {@code segment.getClusterGroups().getTuples()} selected by this
   * matcher. Returns {@code null} when this matcher is incompatible with the segment's clustering scheme (e.g. none
   * of the configured patterns' columns or virtual columns resolve against the segment's clustering signature),
   * meaning the matcher cannot meaningfully reason about this segment's content. Returns an empty list when the
   * matcher applies (its patterns are resolvable) but no configured pattern matches any tuple; the matcher can
   * reason about the segment but found no positive content.
   */
  @Nullable
  protected abstract List<Integer> resolveClusterGroupIndices(DataSegment segment);

  /**
   * Returns the load spec for the resolved cluster-group indices, or null when this matcher has nothing to
   * contribute for the given segment.
   *
   * <p>Null is returned when:
   * <ul>
   *   <li>the segment is not clustered,</li>
   *   <li>the matcher's patterns are incompatible with the segment's clustering scheme (see
   *       {@link #resolveClusterGroupIndices}), or</li>
   *   <li>no configured pattern matches the segment's tuples <em>and</em> the segment is not a core partition
   *       (i.e. {@code partitionNum >= numCorePartitions}). The empty load is only useful to keep the broker's
   *       shard-group completeness check happy, and that check applies only to the core partition group; appended
   *       segments are queried individually and don't need an empty stub when no positive content matches.</li>
   * </ul>
   *
   * <p>When the segment is a core partition of a clustered shard group, the matcher is compatible, and no pattern
   * matches any tuple, returns the "empty" load (same {@code partialClusterGroup} type with an empty index list
   * and {@link #EMPTY_LOAD_FINGERPRINT}). The historical-side loader honors it by performing no load, leaving the
   * segment uniformly placed in the timeline alongside its positively-matched siblings so the broker treats the
   * group as complete.
   */
  @Override
  @Nullable
  public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
  {
    if (segment.getClusterGroups() == null) {
      return null;
    }
    final List<Integer> resolved = resolveClusterGroupIndices(segment);
    if (resolved == null) {
      // Matcher is incompatible with this segment's clustering scheme. Treat as opaque so the rule's cannot-match
      // handling takes over rather than dispatching a stub empty load.
      return null;
    }
    final ShardSpec shardSpec = segment.getShardSpec();
    if (resolved.isEmpty() && shardSpec.getPartitionNum() >= shardSpec.getNumCorePartitions()) {
      // No patterns match and this segment isn't part of a core partition group, so no need for an empty load. Fall
      // through to the cannot-match handling.
      return null;
    }
    final String fingerprint = resolved.isEmpty() ? EMPTY_LOAD_FINGERPRINT : computeFingerprint(resolved);
    return new MatchResult(PartialClusterGroupLoadSpec.wireForm(baseLoadSpec, resolved, fingerprint), fingerprint);
  }

  static String computeFingerprint(List<Integer> sortedDedupedIndices)
  {
    final Hasher hasher = Hashing.sha256().newHasher();
    for (Integer idx : sortedDedupedIndices) {
      hasher.putInt(idx);
    }
    final String hex = BaseEncoding.base16().encode(hasher.hash().asBytes()).toLowerCase(Locale.ROOT);
    // should be good enough without dragging the whole thing around for every segment
    return FINGERPRINT_VERSION + ":" + hex.substring(0, 16);
  }
}
