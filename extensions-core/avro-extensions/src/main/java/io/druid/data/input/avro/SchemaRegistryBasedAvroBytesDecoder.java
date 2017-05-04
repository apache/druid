/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.druid.java.util.common.parsers.ParseException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.nio.ByteBuffer;

public class SchemaRegistryBasedAvroBytesDecoder implements AvroBytesDecoder
{
  private final SchemaRegistryClient registry;

  @JsonCreator
  public SchemaRegistryBasedAvroBytesDecoder(
      @JsonProperty("url") String url,
      @JsonProperty("capacity") Integer capacity
  )
  {
    int identityMapCapacity = capacity == null ? Integer.MAX_VALUE : capacity;
    this.registry = new CachedSchemaRegistryClient(url, identityMapCapacity);
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
    try {
      bytes.get(); // ignore first \0 byte
      int id = bytes.getInt(); // extract schema registry id
      int length = bytes.limit() - 1 - 4;
      int offset = bytes.position() + bytes.arrayOffset();
      Schema schema = registry.getByID(id);
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      return reader.read(null, DecoderFactory.get().binaryDecoder(bytes.array(), offset, length, null));
    }
    catch (Exception e) {
      throw new ParseException(e, "Fail to decode avro message!");
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

    return registry != null ? registry.equals(that.registry) : that.registry == null;
  }

  @Override
  public int hashCode()
  {
    return registry != null ? registry.hashCode() : 0;
  }
}
