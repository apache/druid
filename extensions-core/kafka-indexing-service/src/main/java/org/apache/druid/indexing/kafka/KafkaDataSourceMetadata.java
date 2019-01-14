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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;

import java.util.Map;

public class KafkaDataSourceMetadata extends SeekableStreamDataSourceMetadata<Integer, Long>
{

  @JsonCreator
  public KafkaDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamPartitions<Integer, Long> kafkaPartitions
  )
  {
    super(kafkaPartitions);
  }

  @Override
  protected SeekableStreamDataSourceMetadata<Integer, Long> createConcreteDataSourceMetaData(
      String streamId,
      Map<Integer, Long> newMap
  )
  {
    return new KafkaDataSourceMetadata(new SeekableStreamPartitions<>(streamId, newMap));
  }
}
