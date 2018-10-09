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


import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.firehose.ChatHandler;

import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Interface for abstracting the indexing task run logic. Only used by Kafka indexing tasks,
 * but will also be used by Kinesis indexing tasks once implemented
 *
 * @param <partitionType> Partition Number Type
 * @param <sequenceType> Sequence Number Type
 */
public interface SeekableStreamIndexTaskRunner<partitionType, sequenceType> extends ChatHandler
{
  Appenderator getAppenderator();

  TaskStatus run(TaskToolbox toolbox);

  void stopGracefully();

  @VisibleForTesting
  RowIngestionMeters getRowIngestionMeters();

  @VisibleForTesting
  SeekableStreamIndexTask.Status getStatus();

  @VisibleForTesting
  Map<partitionType, sequenceType> getCurrentOffsets();

  @VisibleForTesting
  Map<partitionType, sequenceType> getEndOffsets();

  @VisibleForTesting
  Response setEndOffsets(
      Map<partitionType, sequenceType> offsets,
      boolean finish // this field is only for internal purposes, shouldn't be usually set by users
  ) throws InterruptedException;

  @VisibleForTesting
  Response pause() throws InterruptedException;

  @VisibleForTesting
  void resume() throws InterruptedException;
}
