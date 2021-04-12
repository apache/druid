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

package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SchemaRegistryBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{

  private static final Logger LOGGER = new Logger(SchemaRegistryBasedProtobufBytesDecoder.class);

  private final SchemaRegistryClient registry;
  private int identityMapCapacity;

  @JsonCreator
  public SchemaRegistryBasedProtobufBytesDecoder(
      @JsonProperty("url") @Deprecated String url,
      @JsonProperty("capacity") Integer capacity,
      @JsonProperty("urls") @Nullable List<String> urls,
      @JsonProperty("config") @Nullable Map<String, ?> config,
      @JsonProperty("headers") @Nullable Map<String, String> headers
  )
  {
    this.identityMapCapacity = capacity == null ? Integer.MAX_VALUE : capacity;
    if (url != null && !url.isEmpty()) {
      this.registry = new CachedSchemaRegistryClient(Collections.singletonList(url), identityMapCapacity, Collections.singletonList(new ProtobufSchemaProvider()), config, headers);
    } else {
      this.registry = new CachedSchemaRegistryClient(urls, identityMapCapacity, Collections.singletonList(new ProtobufSchemaProvider()), config, headers);
    }
  }

  @VisibleForTesting
  int getIdentityMapCapacity()
  {
    return this.identityMapCapacity;
  }

  @VisibleForTesting
  SchemaRegistryBasedProtobufBytesDecoder(SchemaRegistryClient registry)
  {
    this.registry = registry;
  }

  @Override
  public DynamicMessage parse(ByteBuffer bytes)
  {
    bytes.get(); // ignore first \0 byte
    int id = bytes.getInt(); // extract schema registry id
    bytes.get(); // ignore \0 byte before PB message
    int length = bytes.limit() - 2 - 4;
    Descriptors.Descriptor descriptor;
    try {
      ProtobufSchema schema = (ProtobufSchema) registry.getSchemaById(id);
      descriptor = schema.toDescriptor();
    }
    catch (RestClientException e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(e, "Fail to get protobuf schema because of can not connect to registry or failed http request!");
    }
    catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(e, "Fail to get protobuf schema because of invalid schema!");
    }
    try {
      byte[] rawMessage = new byte[length];
      bytes.get(rawMessage, 0, length);
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, rawMessage);
      return message;
    }
    catch (Exception e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(e, "Fail to decode protobuf message!");
    }
  }
}
