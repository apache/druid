/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.indexing.kafka;

import com.google.common.annotations.VisibleForTesting;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.stats.RowIngestionMeters;
import io.druid.indexing.kafka.KafkaIndexTask.Status;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.firehose.ChatHandler;

import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * This class is used by only {@link KafkaIndexTask}. We currently have two implementations of this interface, i.e.,
 * {@link IncrementalPublishingKafkaIndexTaskRunner} and {@link LegacyKafkaIndexTaskRunner}. The latter one was used in
 * the versions prior to 0.12.0, but being kept to support rolling update from them.
 *
 * We don't have a good reason for having this interface except for better code maintenance for the latest kakfa
 * indexing algorithm. As a result, this interface can be removed in the future when {@link LegacyKafkaIndexTaskRunner}
 * is removed and it's no longer useful.
 */
public interface KafkaIndexTaskRunner extends ChatHandler
{
  Appenderator getAppenderator();

  TaskStatus run(TaskToolbox toolbox);

  void stopGracefully();

  // The below methods are mostly for unit testing.

  @VisibleForTesting
  RowIngestionMeters getRowIngestionMeters();
  @VisibleForTesting
  Status getStatus();

  @VisibleForTesting
  Map<Integer, Long> getCurrentOffsets();
  @VisibleForTesting
  Map<Integer, Long> getEndOffsets();
  @VisibleForTesting
  Response setEndOffsets(
      Map<Integer, Long> offsets,
      boolean finish // this field is only for internal purposes, shouldn't be usually set by users
  ) throws InterruptedException;

  @VisibleForTesting
  Response pause() throws InterruptedException;
  @VisibleForTesting
  void resume() throws InterruptedException;
}
