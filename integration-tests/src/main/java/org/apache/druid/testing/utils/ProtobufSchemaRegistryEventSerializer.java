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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.IntegrationTestingConfig;

import java.nio.ByteBuffer;
import java.util.List;

public class ProtobufSchemaRegistryEventSerializer extends ProtobufEventSerializer
{
  private static final int MAX_INITIALIZE_RETRIES = 10;
  public static final String TYPE = "protobuf-schema-registry";

  private final IntegrationTestingConfig config;
  private final CachedSchemaRegistryClient client;
  private int schemaId = -1;


  @JsonCreator
  public ProtobufSchemaRegistryEventSerializer(
      @JacksonInject IntegrationTestingConfig config
  )
  {
    this.config = config;
    this.client = new CachedSchemaRegistryClient(
        StringUtils.format("http://%s", config.getSchemaRegistryHost()),
        Integer.MAX_VALUE,
        ImmutableMap.of(
            "basic.auth.credentials.source", "USER_INFO",
            "basic.auth.user.info", "druid:diurd"
        ),
        ImmutableMap.of()
    );

  }

  @Override
  public void initialize(String topic)
  {
    try {
      RetryUtils.retry(
          () -> {
            schemaId = client.register(topic, new ProtobufSchema(ProtobufEventSerializer.SCHEMA.newMessageBuilder("Wikipedia").getDescriptorForType()));
            return 0;
          },
          (e) -> true,
          MAX_INITIALIZE_RETRIES
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] serialize(List<Pair<String, Object>> event)
  {
    DynamicMessage.Builder builder = SCHEMA.newMessageBuilder("Wikipedia");
    Descriptors.Descriptor msgDesc = builder.getDescriptorForType();
    for (Pair<String, Object> pair : event) {
      builder.setField(msgDesc.findFieldByName(pair.lhs), pair.rhs);
    }
    byte[] bytes = builder.build().toByteArray();
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 6).put((byte) 0).putInt(schemaId).put((byte) 0).put(bytes);
    bb.rewind();
    return bb.array();
  }
}
