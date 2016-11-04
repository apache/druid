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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 */
public class InlineSchemaAvroBytesDecoder implements AvroBytesDecoder
{
  private static final Logger logger = new Logger(InlineSchemaAvroBytesDecoder.class);

  private final Schema schemaObj;
  private final Map<String, Object> schema;

  @JsonCreator
  public InlineSchemaAvroBytesDecoder(
      @JacksonInject @Json ObjectMapper mapper,
      @JsonProperty("schema") Map<String, Object> schema
  ) throws Exception
  {
    Preconditions.checkArgument(schema != null, "schema must be provided");

    this.schema = schema;
    String schemaStr = mapper.writeValueAsString(schema);;

    logger.info("Schema string [%s]", schemaStr);
    schemaObj = new Schema.Parser().parse(schemaStr);
  }

  //For UT only
  @VisibleForTesting
  InlineSchemaAvroBytesDecoder(Schema schemaObj)
  {
    this.schemaObj = schemaObj;
    this.schema = null;
  }

  @JsonProperty
  public Map<String, Object> getSchema()
  {
    return schema;
  }

  @Override
  public GenericRecord parse(ByteBuffer bytes)
  {
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schemaObj);
    ByteBufferInputStream inputStream = new ByteBufferInputStream(Collections.singletonList(bytes));
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    }
    catch (Exception e) {
      throw new ParseException(e, "Fail to decode avro message!");
    }
  }
}
