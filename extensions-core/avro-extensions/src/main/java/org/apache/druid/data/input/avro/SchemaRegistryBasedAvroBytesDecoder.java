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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SchemaRegistryBasedAvroBytesDecoder implements AvroBytesDecoder
{
  private final SchemaRegistryClient registry;

  @JsonCreator
  public SchemaRegistryBasedAvroBytesDecoder(
      @JsonProperty("url") @Deprecated String url,
      @JsonProperty("capacity") Integer capacity,
      @JsonProperty("urls") @Nullable List<String> urls,
      @JsonProperty("config") @Nullable Map<String, ?> config,
      @JsonProperty("headers") @Nullable Map<String, String> headers
  )
  {
    int identityMapCapacity = capacity == null ? Integer.MAX_VALUE : capacity;
    if (url != null && !url.isEmpty()) {
      this.registry = new CachedSchemaRegistryClient(url, identityMapCapacity, config, headers);
    } else {
      this.registry = new CachedSchemaRegistryClient(urls, identityMapCapacity, config, headers);
    }
  }

  //For UT only
  @VisibleForTesting
  SchemaRegistryBasedAvroBytesDecoder(SchemaRegistryClient registry)
  {
    this.registry = registry;
  }

  @Override
  public GenericRecord parse(ByteBuffer bytes)
  {
    int length = bytes.limit() - 1 - 4;
    if (length < 0) {
      throw new ParseException("Failed to decode avro message, not enough bytes to decode (%s)", bytes.limit());
    }

    bytes.get(); // ignore first \0 byte
    int id = bytes.getInt(); // extract schema registry id
    int offset = bytes.position() + bytes.arrayOffset();
    Schema schema;

    try {
      ParsedSchema parsedSchema = registry.getSchemaById(id);
      schema = parsedSchema instanceof AvroSchema ? ((AvroSchema) parsedSchema).rawSchema() : null;
    }
    catch (IOException | RestClientException ex) {
      throw new RE(ex, "Failed to get Avro schema: %s", id);
    }
    if (schema == null) {
      throw new RE("Failed to find Avro schema: %s", id);
    }
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(bytes.array(), offset, length, null));
    }
    catch (Exception e) {
      throw new ParseException(e, "Fail to decode Avro message for schema: %s!", id);
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

    SchemaRegistryBasedAvroBytesDecoder that = (SchemaRegistryBasedAvroBytesDecoder) o;

    return Objects.equals(registry, that.registry);
  }

  @Override
  public int hashCode()
  {
    return registry != null ? registry.hashCode() : 0;
  }
}
