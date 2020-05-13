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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Map;
import java.util.Objects;

public class AvroOCFInputFormat extends NestedInputFormat
{
  private static final Logger LOGGER = new Logger(AvroOCFInputFormat.class);

  private final boolean binaryAsString;
  @Nullable
  private final Schema readerSchema;

  @JsonCreator
  public AvroOCFInputFormat(
      @JacksonInject @Json ObjectMapper mapper,
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("schema") @Nullable Map<String, Object> schema,
      @JsonProperty("binaryAsString") @Nullable Boolean binaryAsString
  ) throws Exception
  {
    super(flattenSpec);
    // If a reader schema is supplied create the datum reader with said schema, otherwise use the writer schema
    if (schema != null) {
      String schemaStr = mapper.writeValueAsString(schema);
      LOGGER.debug("Initialising with reader schema: [%s]", schemaStr);
      this.readerSchema = new Schema.Parser().parse(schemaStr);
    } else {
      this.readerSchema = null;
    }
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;
  }

  @Override
  public boolean isSplittable()
  {
    // In the future Avro OCF files could be split, the format allows for efficient splitting
    // See https://avro.apache.org/docs/current/spec.html#Object+Container+Files for details
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new AvroOCFReader(
        inputRowSchema,
        source,
        temporaryDirectory,
        readerSchema,
        getFlattenSpec(),
        binaryAsString
    );
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
    if (!super.equals(o)) {
      return false;
    }
    AvroOCFInputFormat that = (AvroOCFInputFormat) o;
    return binaryAsString == that.binaryAsString &&
           Objects.equals(readerSchema, that.readerSchema);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), binaryAsString, readerSchema);
  }
}
