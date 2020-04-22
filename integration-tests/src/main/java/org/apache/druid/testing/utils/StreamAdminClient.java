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

package org.apache.druid.testing.utils;

import java.util.Map;

/**
 * This interface provides the administrative client contract for any stream storage (such as Kafka and Kinesis)
 * which supports managing and inspecting streams (aka topics) and stream's partitions (aka shards).
 * This is used for setting up, tearing down and any other administrative changes required in integration tests.
 * Each method resulting in a change of state for the stream is intended to be synchronous to help
 * make integration tests deterministic and easy to write.
 */
public interface StreamAdminClient
{
  void createStream(String streamName, int partitionCount, Map<String, String> tags) throws Exception;

  void deleteStream(String streamName) throws Exception;

  void updatePartitionCount(String streamName, int newPartitionCount, boolean blocksUntilStarted) throws Exception;

  boolean isStreamActive(String streamName);

  int getStreamPartitionCount(String streamName) throws Exception;

  boolean verfiyPartitionCountUpdated(String streamName, int oldPartitionCount, int newPartitionCount) throws Exception;
}
