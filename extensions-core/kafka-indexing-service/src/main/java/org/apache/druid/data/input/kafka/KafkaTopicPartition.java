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
import org.apache.druid.error.DruidException;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * This class represents the partition id for kafka ingestion. This partition id includes topic name along with an
 * integer partition. The topic name is required in multi-topic mode because the same partition id can be used for
 * different topics.
 * This class is used as a key in {@link org.apache.druid.indexing.kafka.KafkaDataSourceMetadata} to store the offsets
 * for each partition.
 */
@JsonSerialize(using = KafkaTopicPartition.KafkaTopicPartitionSerializer.class, keyUsing =
    KafkaTopicPartition.KafkaTopicPartitionKeySerializer.class)
@JsonDeserialize(using = KafkaTopicPartition.KafkaTopicPartitionDeserializer.class, keyUsing =
    KafkaTopicPartition.KafkaTopicPartitionKeyDeserializer.class)
public class KafkaTopicPartition
{
  private final int partition;
  @Nullable
  private final String topic;

  /**
   * This flag is used to maintain backward compatibilty with older versions of kafka indexing. If this flag
   * is set to false,
   * - KafkaTopicPartition will be serialized as an integer and can be read back by older version.
   * - topic field is ensured to be null.
   * This flag must be explicitly passed while constructing KafkaTopicPartition object. That way, we can ensure that
   * a particular supervisor is always running in multi topic mode or single topic mode.
   */
  private final boolean multiTopicPartition;

  public KafkaTopicPartition(boolean multiTopicPartition, @Nullable String topic, int partition)
  {
    this.partition = partition;
    this.multiTopicPartition = multiTopicPartition;
    if (multiTopicPartition) {
      if (topic == null) {
        throw DruidException.defensive("the topic cannot be null in multi-topic mode of kafka ingestion");
      }
      this.topic = topic;
    } else {
      this.topic = null;
    }
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

  /**
   * A utility method to convert KafkaTopicPartition to {@link TopicPartition} object. For single topic ingestion,
   * the fallback topic is used to populate the topic name in {@link TopicPartition} object.
   */
  public TopicPartition asTopicPartition(String fallbackTopic)
  {
    return new TopicPartition(topic != null ? topic : fallbackTopic, partition);
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
    return partition == that.partition && multiTopicPartition == that.multiTopicPartition && Objects.equals(
        topic,
        that.topic
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partition, multiTopicPartition, topic);
  }

  @Override
  public String toString()
  {
    return "KafkaTopicPartition{" +
           "partition=" + partition +
           ", topic='" + topic + '\'' +
           ", multiTopicPartition=" + multiTopicPartition +
           '}';
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
      if (null != value.topic && value.multiTopicPartition) {
        gen.writeString(value.topic + ":" + value.partition);
      } else {
        gen.writeString(String.valueOf(value.partition));
      }
    }

    @Override
    public Class<KafkaTopicPartition> handledType()
    {
      return KafkaTopicPartition.class;
    }
  }

  public static class KafkaTopicPartitionKeySerializer extends JsonSerializer<KafkaTopicPartition>
  {
    @Override
    public void serialize(KafkaTopicPartition value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException
    {
      if (null != value.topic && value.multiTopicPartition) {
        gen.writeFieldName(value.topic + ":" + value.partition);
      } else {
        gen.writeFieldName(String.valueOf(value.partition));
      }
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
    int index = str.lastIndexOf(':');
    if (index < 0) {
      return new KafkaTopicPartition(false, null, Integer.parseInt(str));
    } else {
      return new KafkaTopicPartition(
          true,
          str.substring(0, index),
          Integer.parseInt(str.substring(index + 1))
      );
    }
  }
}
