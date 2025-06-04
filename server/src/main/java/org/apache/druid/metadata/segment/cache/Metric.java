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
   * Total number of pending segments currently present in the metadata store.
   */
  public static final String PERSISTED_PENDING_SEGMENTS = "segment/pending/count";

  // CACHE METRICS
  private static final String METRIC_NAME_PREFIX = "segment/metadataCache/";

  /**
   * Number of read-write transactions performed on the cache for a datasource.
   */
  public static final String READ_WRITE_TRANSACTIONS = METRIC_NAME_PREFIX + "transactions/readWrite";

  /**
   * Number of write-only transactions performed on the cache for a datasource.
   */
  public static final String WRITE_ONLY_TRANSACTIONS = METRIC_NAME_PREFIX + "transactions/writeOnly";

  /**
   * Number of read-only transactions performed on the cache for a datasource.
   */
  public static final String READ_ONLY_TRANSACTIONS = METRIC_NAME_PREFIX + "transactions/readOnly";

  /**
   * Total number of distinct intervals currently present in the cache.
   */
  public static final String CACHED_INTERVALS = METRIC_NAME_PREFIX + "interval/count";

  /**
   * Total number of segments currently present in the cache.
   */
  public static final String CACHED_USED_SEGMENTS = METRIC_NAME_PREFIX + "used/count";

  /**
   * Total number of segments currently present in the cache.
   */
  public static final String CACHED_UNUSED_SEGMENTS = METRIC_NAME_PREFIX + "unused/count";

  /**
   * Total number of pending segments currently present in the cache.
   */
  public static final String CACHED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/count";


  // CACHE SYNC TIME METRICS

  /**
   * Time taken in milliseconds for the latest sync with metadata store.
   */
  public static final String SYNC_DURATION_MILLIS = METRIC_NAME_PREFIX + "sync/time";

  /**
   * Time taken in milliseconds to update all segment IDs in the cache.
   */
  public static final String UPDATE_IDS_DURATION_MILLIS = METRIC_NAME_PREFIX + "updateIds/time";

  /**
   * Time taken in milliseconds to fetch payloads of used segments from the metadata store.
   */
  public static final String RETRIEVE_SEGMENT_PAYLOADS_DURATION_MILLIS = METRIC_NAME_PREFIX + "fetchPayloads/time";

  /**
   * Time taken in milliseconds to fetch all segment IDs from the metadata store.
   */
  public static final String RETRIEVE_SEGMENT_IDS_DURATION_MILLIS = METRIC_NAME_PREFIX + "fetchIds/time";

  /**
   * Time taken in milliseconds to fetch all pending segments from the metadata store.
   */
  public static final String RETRIEVE_PENDING_SEGMENTS_DURATION_MILLIS = METRIC_NAME_PREFIX + "fetchPending/time";

  /**
   * Time taken in milliseconds to fetch all segment schemas from the metadata store.
   */
  public static final String RETRIEVE_SEGMENT_SCHEMAS_DURATION_MILLIS = METRIC_NAME_PREFIX + "fetchSchemas/time";

  /**
   * Time taken to update the datasource snapshot in the cache.
   */
  public static final String UPDATE_SNAPSHOT_DURATION_MILLIS = METRIC_NAME_PREFIX + "updateSnapshot/time";


  // CACHE UPDATE METRICS

  /**
   * Total number of datasources removed from the cache if they have no segments anymore.
   */
  public static final String DELETED_DATASOURCES = METRIC_NAME_PREFIX + "dataSource/deleted";

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
   * Number of pending segments updated in the cache from the metadata store in the latest sync.
   */
  public static final String UPDATED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/updated";

  /**
   * Number of unparseable segment records skipped while refreshing the cache.
   */
  public static final String SKIPPED_SEGMENTS = METRIC_NAME_PREFIX + "skipped";

  /**
   * Number of unparseable segment schema records skipped while refreshing the cache.
   */
  public static final String SKIPPED_SEGMENT_SCHEMAS = METRIC_NAME_PREFIX + "schema/skipped";

  /**
   * Number of unparseable pending segment records skipped while refreshing the cache.
   */
  public static final String SKIPPED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "pending/skipped";
}
