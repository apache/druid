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
import java.util.Objects;

public class SchemaRegistryBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{

  private static final Logger LOGGER = new Logger(SchemaRegistryBasedProtobufBytesDecoder.class);

  private final SchemaRegistryClient registry;
  private final String url;
  private final int capacity;
  private final List<String> urls;
  private final Map<String, ?> config;
  private final Map<String, String> headers;

  @JsonCreator
  public SchemaRegistryBasedProtobufBytesDecoder(
      @JsonProperty("url") @Deprecated String url,
      @JsonProperty("capacity") Integer capacity,
      @JsonProperty("urls") @Nullable List<String> urls,
      @JsonProperty("config") @Nullable Map<String, ?> config,
      @JsonProperty("headers") @Nullable Map<String, String> headers
  )
  {
    this.url = url;
    this.capacity = capacity == null ? Integer.MAX_VALUE : capacity;
    this.urls = urls;
    this.config = config;
    this.headers = headers;
    if (url != null && !url.isEmpty()) {
      this.registry = new CachedSchemaRegistryClient(Collections.singletonList(this.url), this.capacity, Collections.singletonList(new ProtobufSchemaProvider()), this.config, this.headers);
    } else {
      this.registry = new CachedSchemaRegistryClient(this.urls, this.capacity, Collections.singletonList(new ProtobufSchemaProvider()), this.config, this.headers);
    }
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
  }

  @JsonProperty
  public int getCapacity()
  {
    return capacity;
  }

  @JsonProperty
  public List<String> getUrls()
  {
    return urls;
  }

  @JsonProperty
  public Map<String, ?> getConfig()
  {
    return config;
  }

  @JsonProperty
  public Map<String, String> getHeaders()
  {
    return headers;
  }

  @VisibleForTesting
  int getIdentityMapCapacity()
  {
    return this.capacity;
  }

  @VisibleForTesting
  SchemaRegistryBasedProtobufBytesDecoder(SchemaRegistryClient registry)
  {
    this.url = null;
    this.capacity = Integer.MAX_VALUE;
    this.urls = null;
    this.config = null;
    this.headers = null;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaRegistryBasedProtobufBytesDecoder that = (SchemaRegistryBasedProtobufBytesDecoder) o;

    return Objects.equals(url, that.url) &&
        Objects.equals(capacity, that.capacity) &&
        Objects.equals(urls, that.urls) &&
        Objects.equals(config, that.config) &&
        Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode()
  {
    int result = url != null ? url.hashCode() : 0;
    result = 31 * result + capacity;
    result = 31 * result + (urls != null ? urls.hashCode() : 0);
    result = 31 * result + (config != null ? config.hashCode() : 0);
    result = 31 * result + (headers != null ? headers.hashCode() : 0);
    return result;
  }
}
