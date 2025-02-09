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

  private static final String METRIC_NAME_PREFIX = "segment/metadataCache/";

  public static final String SYNC_DURATION_MILLIS = METRIC_NAME_PREFIX + "sync/time";

  public static final String POLLED_SEGMENTS = METRIC_NAME_PREFIX + "polled/total";
  public static final String POLLED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "polled/pending";

  public static final String STALE_USED_SEGMENTS = METRIC_NAME_PREFIX + "stale/used";

  public static final String DELETED_SEGMENTS = METRIC_NAME_PREFIX + "deleted/total";
  public static final String DELETED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "deleted/pending";

  public static final String USED_SEGMENTS_UPDATED = METRIC_NAME_PREFIX + "updated/used";
  public static final String UNUSED_SEGMENTS_UPDATED = METRIC_NAME_PREFIX + "updated/unused";
  public static final String PENDING_SEGMENTS_UPDATED = METRIC_NAME_PREFIX + "updated/pending";

  public static final String IGNORED_SEGMENTS = METRIC_NAME_PREFIX + "ignored/total";
  public static final String IGNORED_PENDING_SEGMENTS = METRIC_NAME_PREFIX + "ignored/pending";
}
