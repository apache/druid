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
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;

import java.util.Map;

public class KafkaPartitions extends SeekableStreamPartitions<Integer, Long>
{

  public static final long NO_END_SEQUENCE_NUMBER = Long.MAX_VALUE;

  @JsonCreator
  public KafkaPartitions(
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitionOffsetMap") final Map<Integer, Long> partitionOffsetMap
  )
  {
    super(
        topic,
        partitionOffsetMap
    );

  }

  @Override
  public Long getNoEndSequenceNumber()
  {
    return Long.MAX_VALUE;
  }

  @JsonProperty
  public String getTopic()
  {
    return getId();
  }

  @JsonProperty
  public Map<Integer, Long> getPartitionOffsetMap()
  {
    return getPartitionSequenceMap();
  }

}
