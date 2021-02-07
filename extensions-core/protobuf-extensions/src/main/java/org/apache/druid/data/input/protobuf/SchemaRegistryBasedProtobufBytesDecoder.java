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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.Collections;

public class SchemaRegistryBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{

  private static final Logger LOGGER = new Logger(SchemaRegistryBasedProtobufBytesDecoder.class);

  private final SchemaRegistryClient registry;
  private int identityMapCapacity;

  @JsonCreator
  public SchemaRegistryBasedProtobufBytesDecoder(
      @JsonProperty("url") String url,
      @JsonProperty("capacity") Integer capacity
  )
  {
    this.identityMapCapacity = capacity == null ? Integer.MAX_VALUE : capacity;
    registry = new CachedSchemaRegistryClient(Collections.singletonList(url), identityMapCapacity, Collections.singletonList(new ProtobufSchemaProvider()), null);
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
    catch (Exception e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(e, "Fail to get protobuf schema!");
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
