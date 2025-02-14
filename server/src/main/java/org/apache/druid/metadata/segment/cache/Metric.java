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

package org.apache.druid.metadata.segment.cache;

/**
 * Segment metadata cache metric names.
 */
public class Metric
{
  private Metric()
  {
    // no instantiation
  }

  /**
   * Total number of segments currently present in the metadata store.
   */
  public static final String PERSISTED_USED_SEGMENTS = "segment/used/count";

  /**
   * Total number of segments currently present in the metadata store.
   */
  public static final String PERSISTED_UNUSED_SEGMENTS = "segment/unused/count";

  /**
   * Total number of pending segments currently present in the metadata store.
   */
  public static final String PERSISTED_PENDING_SEGMENTS = "segment/pending/count";

  // CACHE METRICS
  private static final String METRIC_NAME_PREFIX = "segment/metadataCache/";

  /**
   * Number of transactions performed on the cache for a datasource.
   */
  public static final String TRANSACTION_COUNT = "transaction/count";

  /**
   * Time taken in milliseconds for the latest sync with metadata store.
   */
  public static final String SYNC_DURATION_MILLIS = METRIC_NAME_PREFIX + "sync/time";

  /**
   * Number of segments which are now stale in the cache and need to be refreshed.
   */
  public static final String STALE_USED_SEGMENTS = METRIC_NAME_PREFIX + "used/stale";

  /**
   * Total number of segments deleted from the cache in the latest sync.
   */
  public static final String DELETED_SEGMENTS = METRIC_NAME_PREFIX + "deleted";

  /**
   * Total number of pending segments deleted from the cache in the latest sync.
   */
  public static final String DELETED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/deleted";

  /**
   * Number of used segments updated in the cache from the metadata store in the latest sync.
   */
  public static final String UPDATED_USED_SEGMENTS = METRIC_NAME_PREFIX + "used/updated";

  /**
   * Number of unused segments updated in the cache from the metadata store in the latest sync.
   */
  public static final String UPDATED_UNUSED_SEGMENTS = METRIC_NAME_PREFIX + "unused/updated";

  /**
   * Number of pending segments updated in the cache from the metadata store in the latest sync.
   */
  public static final String UPDATED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/updated";

  /**
   * Number of unparseable segment records skipped while refreshing the cache.
   */
  public static final String SKIPPED_SEGMENTS = METRIC_NAME_PREFIX + "skipped";

  /**
   * Number of unparseable pending segment records skipped while refreshing the cache.
   */
  public static final String SKIPPED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/skipped";
}
