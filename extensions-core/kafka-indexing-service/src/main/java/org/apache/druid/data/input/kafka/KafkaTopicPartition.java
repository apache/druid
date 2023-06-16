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

package org.apache.druid.data.input.kafka;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * This class represents the partition id for kafka ingestion. This partition id includes topic name along with an
 * integer partition. The topic name is required because the same partition id can be used for different topics.
 * This class is used as a key in {@link org.apache.druid.indexing.kafka.KafkaDataSourceMetadata} to store the offsets
 * for each partition.
 *
 */
@JsonSerialize(using = KafkaTopicPartition.KafkaTopicPartitionSerializer.class,
    keyUsing = KafkaTopicPartition.KafkaTopicPartitionSerializer.class)
@JsonDeserialize(using = KafkaTopicPartition.KafkaTopicPartitionDeserializer.class, keyUsing =
    KafkaTopicPartition.KafkaTopicPartitionKeyDeserializer.class)
public class KafkaTopicPartition
{
  private final int partition;
  @Nullable
  private final String topic;

  // This flag is used to maintain backward incompatibilty with older versions of kafka indexing. If this flag
  // is set to false,
  // - KafkaTopicPartition will be serialized as an integer and can be read back by older version.
  // - topic field is ignored while comparing two KafkaTopicPartition objects and calculating hashcode.
  // This flag must be explicitly passed while constructing KafkaTopicPartition object. That way, we can ensure that
  // a particular supervisor is always running in multi topic mode or single topic mode.
  private final boolean multiTopicPartition;

  public KafkaTopicPartition(boolean multiTopicPartition, @Nullable String topic, int partition)
  {
    this.partition = partition;
    this.topic = topic;
    this.multiTopicPartition = multiTopicPartition;
  }

  public int partition()
  {
    return partition;
  }

  public Optional<String> topic()
  {
    return Optional.ofNullable(topic);
  }

  public boolean isMultiTopicPartition()
  {
    return multiTopicPartition;
  }

  public TopicPartition asTopicPartition(String fallbackTopic)
  {
    return new TopicPartition(topic != null ? topic : fallbackTopic, partition);
  }

  @Override
  public String toString()
  {
    // TODO - fix this so toString is not used for serialization
    if (null != topic && multiTopicPartition) {
      return partition + ":" + topic;
    } else {
      return Integer.toString(partition);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaTopicPartition that = (KafkaTopicPartition) o;
    return partition == that.partition && multiTopicPartition == that.multiTopicPartition && (!multiTopicPartition
                                                                                              || Objects.equals(
        topic,
        that.topic
    ));
  }

  @Override
  public int hashCode()
  {
    if (multiTopicPartition) {
      return Objects.hash(partition, multiTopicPartition, topic);
    } else {
      return Objects.hash(partition, multiTopicPartition);
    }

  }

  public static class KafkaTopicPartitionDeserializer extends JsonDeserializer<KafkaTopicPartition>
  {
    @Override
    public KafkaTopicPartition deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException
    {
      return fromString(p.getValueAsString());
    }

    @Override
    public Class<KafkaTopicPartition> handledType()
    {
      return KafkaTopicPartition.class;
    }
  }

  public static class KafkaTopicPartitionSerializer extends JsonSerializer<KafkaTopicPartition>
  {
    @Override
    public void serialize(KafkaTopicPartition value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException
    {
      gen.writeString(value.toString());
    }

    @Override
    public Class<KafkaTopicPartition> handledType()
    {
      return KafkaTopicPartition.class;
    }
  }

  public static class KafkaTopicPartitionKeyDeserializer extends KeyDeserializer
  {
    @Override
    public KafkaTopicPartition deserializeKey(String key, DeserializationContext ctxt)
    {
      return fromString(key);
    }
  }

  public static KafkaTopicPartition fromString(String str)
  {
    int index = str.indexOf(':');
    if (index < 0) {
      return new KafkaTopicPartition(false, null, Integer.parseInt(str));
    } else {
      return new KafkaTopicPartition(
          true,
          str.substring(index + 1),
          Integer.parseInt(str.substring(0, index))
      );
    }
  }
}
