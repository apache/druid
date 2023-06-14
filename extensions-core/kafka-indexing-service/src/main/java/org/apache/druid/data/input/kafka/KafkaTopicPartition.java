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

@JsonSerialize(using = KafkaTopicPartition.KafkaTopicPartitionSerializer.class,
    keyUsing = KafkaTopicPartition.KafkaTopicPartitionSerializer.class)
@JsonDeserialize(using = KafkaTopicPartition.KafkaTopicPartitionDeserializer.class, keyUsing =
    KafkaTopicPartition.KafkaTopicPartitionKeyDeserializer.class)
public class KafkaTopicPartition
{
  private int hash = 0;
  private final int partition;
  private final Optional<String> topic;

  public KafkaTopicPartition(@Nullable String topic, int partition)
  {
    this.partition = partition;
    this.topic = Optional.ofNullable(topic);
  }

  public int partition()
  {
    return partition;
  }

  public Optional<String> topic()
  {
    return topic;
  }

  public TopicPartition asTopicPartition(String fallbackTopic)
  {
    return new TopicPartition(topic.orElse(fallbackTopic), partition);
  }

  public static KafkaTopicPartition fromTopicPartition(TopicPartition tp)
  {
    return new KafkaTopicPartition(tp.topic(), tp.partition());
  }

  @Override
  public int hashCode()
  {
    if (hash != 0) {
      return hash;
    }
    final int prime = 31;
    int result = prime + partition;
    if (topic.isPresent()) {
      result = prime * result + Objects.hashCode(topic);
    }
    this.hash = result;
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    KafkaTopicPartition other = (KafkaTopicPartition) obj;
    return partition == other.partition && Objects.equals(topic, other.topic);
  }

  @Override
  public String toString()
  {
    if (topic.isPresent()) {
      return partition + ":" + topic.get();
    } else {
      return Integer.toString(partition);
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
      return new KafkaTopicPartition(null, Integer.parseInt(str));
    } else {
      return new KafkaTopicPartition(
          str.substring(index + 1),
          Integer.parseInt(str.substring(0, index))
      );
    }
  }
}
