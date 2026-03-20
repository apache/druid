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

package org.apache.druid.indexing.compact;

import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.segment.IndexIO;
import org.joda.time.Interval;

/**
 * Factory for creating {@link DruidInputSource} for a given datasource and
 * interval used by {@link CompactionJobTemplate}.
 */
public class DruidInputSourceFactory
{
  private final IndexIO indexIO;
  private final TaskConfig taskConfig;
  private final CoordinatorClient coordinatorClient;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;

  @Inject
  public DruidInputSourceFactory(
      IndexIO indexIO,
      TaskConfig taskConfig,
      CoordinatorClient coordinatorClient,
      SegmentCacheManagerFactory segmentCacheManagerFactory
  )
  {
    this.indexIO = indexIO;
    this.coordinatorClient = coordinatorClient;
    this.segmentCacheManagerFactory = segmentCacheManagerFactory;
    this.taskConfig = taskConfig;
  }

  /**
   * Creates a new {@link DruidInputSource} for the given {@code dataSource} and
   * {@code interval}.
   */
  public DruidInputSource create(String dataSource, Interval interval)
  {
    return new DruidInputSource(
        dataSource,
        interval,
        null,
        null,
        null,
        null,
        indexIO,
        coordinatorClient,
        segmentCacheManagerFactory,
        taskConfig
    );
  }
}
