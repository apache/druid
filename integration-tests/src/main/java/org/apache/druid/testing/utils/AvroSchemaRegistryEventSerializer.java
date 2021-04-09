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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.IntegrationTestingConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AvroSchemaRegistryEventSerializer extends AvroEventSerializer
{
  private static final int MAX_INITIALIZE_RETRIES = 10;
  public static final String TYPE = "avro-schema-registry";

  private final IntegrationTestingConfig config;
  private final CachedSchemaRegistryClient client;
  private int schemaId = -1;

  private Schema fromRegistry;

  @JsonCreator
  public AvroSchemaRegistryEventSerializer(
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
            schemaId = client.register(topic, AvroEventSerializer.SCHEMA);
            fromRegistry = client.getById(schemaId);
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
  public byte[] serialize(List<Pair<String, Object>> event) throws IOException
  {
    final WikipediaRecord record = new WikipediaRecord(fromRegistry);
    event.forEach(pair -> record.put(pair.lhs, pair.rhs));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x0);
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    DatumWriter<Object> writer = new GenericDatumWriter<>(fromRegistry);
    writer.write(record, encoder);
    encoder.flush();
    byte[] bytes = out.toByteArray();
    out.close();
    return bytes;
  }
}
