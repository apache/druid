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

package org.apache.druid.server.compaction;

/**
 * Stable code identifying why an interval was not compacted in a run. Reported
 * as a dimension on the {@code skipCompact} metrics and as the breakdown of
 * skipped stats in {@code AutoCompactionSnapshot}, so the name of a constant
 * must not change once released.
 * <p>
 * Every reason has a {@link Category} which tells a consumer how to treat the
 * interval without having to know the individual reasons. Consumers must switch
 * on the category rather than on the reason so that reasons added later are
 * handled correctly by default.
 */
public enum CompactionSkipReason
{
  /**
   * Interval falls within {@code skipOffsetFromLatest} or a configured
   * {@code skipIntervals}.
   */
  SKIP_OFFSET(Category.OUT_OF_SCOPE),

  /**
   * Interval is locked by another task, so compaction cannot claim it right now.
   */
  INTERVAL_LOCKED(Category.TRANSIENT),

  /**
   * Segment timeline has not been refreshed since the last compaction task for
   * this interval succeeded, so its current state is not yet known.
   */
  TIMELINE_NOT_UPDATED(Category.TRANSIENT),

  /**
   * Compaction task for this interval could not acquire the locks it needs.
   */
  LOCK_ACQUISITION_FAILED(Category.TRANSIENT),

  /**
   * Interval needs compaction but was filtered out by the
   * {@link CompactionCandidateSearchPolicy}. Relaxing the policy thresholds
   * makes it eligible again.
   */
  REJECTED_BY_SEARCH_POLICY(Category.DEFERRED),

  /**
   * Total size of the segments in the interval exceeds the legacy
   * {@code inputSegmentSizeBytes} of the compaction config.
   */
  INPUT_SEGMENT_SIZE_EXCEEDED(Category.DEFERRED),

  /**
   * Interval contains segments with a partial-eternity interval, which
   * compaction cannot handle.
   *
   * @see <a href="https://github.com/apache/druid/issues/13208">apache/druid#13208</a>
   */
  PARTIAL_ETERNITY_INTERVAL(Category.UNSUPPORTED),

  /**
   * Compaction job created for this interval is not valid for the current
   * cluster or datasource config.
   */
  INVALID_JOB(Category.UNSUPPORTED);

  /**
   * Tells a consumer how to treat a skipped interval. Reporting should be driven
   * by the category so that a newly added {@link CompactionSkipReason} behaves
   * sensibly without the consumer being updated.
   */
  public enum Category
  {
    /**
     * Interval was deliberately excluded by the compaction config. It does not
     * count against the datasource being fully compacted.
     */
    OUT_OF_SCOPE,

    /**
     * Interval could not be compacted in this run but is expected to be picked
     * up in a later run with no operator action.
     */
    TRANSIENT,

    /**
     * Interval does not match the compaction config and will keep being passed
     * over until the compaction config or policy is changed.
     */
    DEFERRED,

    /**
     * Interval cannot be compacted as it currently stands, regardless of config.
     */
    UNSUPPORTED
  }

  private final Category category;

  CompactionSkipReason(Category category)
  {
    this.category = category;
  }

  public Category getCategory()
  {
    return category;
  }
}
