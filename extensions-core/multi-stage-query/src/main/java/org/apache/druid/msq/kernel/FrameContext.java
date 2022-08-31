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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;

import java.io.File;

/**
 * Provides services and objects for the functioning of the frame processors
 */
public interface FrameContext
{
  JoinableFactory joinableFactory();

  GroupByStrategySelector groupByStrategySelector();

  RowIngestionMeters rowIngestionMeters();

  DataSegmentProvider dataSegmentProvider();

  File tempDir();

  ObjectMapper jsonMapper();

  IndexIO indexIO();

  File persistDir();

  DataSegmentPusher segmentPusher();

  IndexMergerV9 indexMerger();

  WorkerMemoryParameters memoryParameters();
}
