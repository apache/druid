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

package org.apache.druid.segment.metadata;

/**
 * Metrics related to {@link SegmentSchemaCache}, {@link SegmentSchemaManager}, and {@link CompactionStateCache}.
 */
public class Metric
{
  private static final String PREFIX = "segment/schemaCache/";
  private static final String COMPACTION_STATE_PREFIX = "segment/compactionStateCache/";

  public static final String CACHE_MISSES = "miss/count";

  // Current contents of the cache
  public static final String REALTIME_SEGMENT_SCHEMAS = PREFIX + "realtime/count";
  public static final String USED_SEGMENT_SCHEMAS = PREFIX + "used/count";
  public static final String COLD_SEGMENT_SCHEMAS = PREFIX + "deepStorageOnly/count";
  public static final String USED_SEGMENT_SCHEMA_FINGERPRINTS = PREFIX + "usedFingerprint/count";
  public static final String SCHEMAS_PENDING_BACKFILL = PREFIX + "pendingBackfill/count";

  // Back-fill metrics
  public static final String BACKFILL_DURATION_MILLIS = PREFIX + "backfill/time";
  public static final String SCHEMAS_BACKFILLED = PREFIX + "backfill/count";

  public static final String COLD_SCHEMA_REFRESH_DURATION_MILLIS = PREFIX + "deepStorageOnly/refresh/time";

  // Broker-side metrics
  public static final String BROKER_POLL_DURATION_MILLIS = PREFIX + "poll/time";
  public static final String BROKER_POLL_FAILED = PREFIX + "poll/failed";
  public static final String BROKER_SEGMENTS_SKIPPED_REFRESH = PREFIX + "refreshSkipped/count";

  // Schema refresh metrics
  public static final String STARTUP_DURATION_MILLIS = "metadatacache/init/time";
  public static final String REFRESHED_SEGMENTS = PREFIX + "refresh/count";
  public static final String REFRESH_SKIPPED_TOMBSTONES = PREFIX + "refresh/tombstone/count";
  public static final String REFRESH_DURATION_MILLIS = PREFIX + "refresh/time";
  public static final String DATASOURCE_REMOVED = PREFIX + "dataSource/removed";

  /**
   * Number of used cold segments in the metadata store.
   */
  public static final String USED_COLD_SEGMENTS = "segment/used/deepStorageOnly/count";

  // Compaction state cache metrics
  public static final String COMPACTION_STATE_CACHE_HITS = COMPACTION_STATE_PREFIX + "hit/count";
  public static final String COMPACTION_STATE_CACHE_MISSES = COMPACTION_STATE_PREFIX + "miss/count";
  public static final String COMPACTION_STATE_CACHE_FINGERPRINTS = COMPACTION_STATE_PREFIX + "fingerprint/count";
}
