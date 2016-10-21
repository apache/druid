/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.schemarepo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.java.util.common.Pair;

import org.schemarepo.api.converter.Converter;
import org.schemarepo.api.converter.IdentityConverter;
import org.schemarepo.api.converter.IntegerConverter;

import java.nio.ByteBuffer;

/**
 * This implementation using injected Kafka topic name as subject name, and an integer as schema id. Before sending avro
 * message to Kafka broker, you need to register the schema to an schema repository, get the schema id, serialized it to
 * 4 bytes and then insert them to the head of the payload. In the reading end, you extract 4 bytes from raw messages,
 * deserialize and return it with the topic name, with which you can lookup the avro schema.
 *
 * @see SubjectAndIdConverter
 */
public class Avro1124SubjectAndIdConverter implements SubjectAndIdConverter<String, Integer>
{
  private final String topic;

  @JsonCreator
  public Avro1124SubjectAndIdConverter(@JsonProperty("topic") String topic)
  {
    this.topic = topic;
  }


  @Override
  public Pair<String, Integer> getSubjectAndId(ByteBuffer payload)
  {
    return new Pair<String, Integer>(topic, payload.getInt());
  }

  @Override
  public void putSubjectAndId(String subject, Integer id, ByteBuffer payload)
  {
    payload.putInt(id);
  }

  @Override
  public Converter<String> getSubjectConverter()
  {
    return new IdentityConverter();
  }

  @Override
  public Converter<Integer> getIdConverter()
  {
    return new IntegerConverter();
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
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

    Avro1124SubjectAndIdConverter converter = (Avro1124SubjectAndIdConverter) o;

    return !(topic != null ? !topic.equals(converter.topic) : converter.topic != null);

  }

  @Override
  public int hashCode()
  {
    return topic != null ? topic.hashCode() : 0;
  }
}
