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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.DynamicConfigProviderUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SchemaRegistryBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{
  public static final String DRUID_DYNAMIC_CONFIG_PROVIDER_KEY = "druid.dynamic.config.provider";

  private static final Logger LOGGER = new Logger(SchemaRegistryBasedProtobufBytesDecoder.class);

  private final SchemaRegistryClient registry;
  private final String url;
  private final int capacity;
  private final List<String> urls;
  private final Map<String, Object> config;
  private final Map<String, Object> headers;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public SchemaRegistryBasedProtobufBytesDecoder(
      @JsonProperty("url") @Deprecated String url,
      @JsonProperty("capacity") Integer capacity,
      @JsonProperty("urls") @Nullable List<String> urls,
      @JsonProperty("config") @Nullable Map<String, Object> config,
      @JsonProperty("headers") @Nullable Map<String, Object> headers,
      @JacksonInject @Json ObjectMapper jsonMapper
  )
  {
    this.url = url;
    this.capacity = capacity == null ? Integer.MAX_VALUE : capacity;
    this.urls = urls;
    this.config = config;
    this.headers = headers;
    this.jsonMapper = jsonMapper;

    if (StringUtils.isNotEmpty(this.url)) {
      LOGGER.warn("\"url\" is deprecated, use \"urls\" instead");
    }

    final var baseUrls = StringUtils.isNotEmpty(this.url) ? Collections.singletonList(this.url) : this.urls;
    if (baseUrls == null) {
      throw new IAE("SchemaRegistryBasedProtobufBytesDecoder requires non-null URLs");
    }

    this.registry = new CachedSchemaRegistryClient(
        baseUrls,
        this.capacity,
        Collections.singletonList(new ProtobufSchemaProvider()),
        DynamicConfigProviderUtils.extraConfigAndSetObjectMap(
            config,
            DRUID_DYNAMIC_CONFIG_PROVIDER_KEY,
            this.jsonMapper
        ),
        DynamicConfigProviderUtils.extraConfigAndSetStringMap(
            headers,
            DRUID_DYNAMIC_CONFIG_PROVIDER_KEY,
            this.jsonMapper
        )
    );
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
  public Map<String, Object> getConfig()
  {
    return config;
  }

  @JsonProperty
  public Map<String, Object> getHeaders()
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
    this.jsonMapper = new ObjectMapper();
  }

  /**
   * Parses a protobuf message from a ByteBuffer that follows the Confluent Schema Registry wire format.
   *
   * <p>The Confluent Schema Registry wire format for protobuf messages is:
   * <pre>
   * [magic_byte(1)] [schema_id(4)] [magic_byte(1)] [protobuf_message(n)]
   * </pre>
   *
   * <p>For more details on the wire format, see:
   * <a href="https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format">
   * Confluent Schema Registry Wire Format Documentation</a>
   *
   * <p>This method extracts the schema ID from the bytes, uses that ID to retrieve the actual schema from the
   * schema registry, and then parses the remaining bytes as a protobuf message using the retrieved schema.
   *
   * @param bytes ByteBuffer containing the Confluent Schema Registry formatted protobuf message
   * @return DynamicMessage parsed from the protobuf bytes
   * @throws ParseException if the schema cannot be retrieved or the message cannot be parsed
   */
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
      throw new ParseException(
          null,
          e,
          "Fail to get protobuf schema because of can not connect to registry or failed http request!"
      );
    }
    catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(null, e, "Fail to get protobuf schema because of invalid schema!");
    }
    try {
      byte[] rawMessage = new byte[length];
      bytes.get(rawMessage, 0, length);
      return DynamicMessage.parseFrom(descriptor, rawMessage);
    }
    catch (Exception e) {
      LOGGER.error(e.getMessage());
      throw new ParseException(null, e, "Fail to decode protobuf message!");
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
