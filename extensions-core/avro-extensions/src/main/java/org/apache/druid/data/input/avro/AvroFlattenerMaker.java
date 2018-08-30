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

import com.google.common.collect.Lists;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AvroFlattenerMaker implements ObjectFlatteners.FlattenerMaker<GenericRecord>
{
  private static final Configuration JSONPATH_CONFIGURATION =
      Configuration.builder()
                   .jsonProvider(new GenericAvroJsonProvider())
                   .mappingProvider(new GenericAvroMappingProvider())
                   .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                   .build();

  private static final EnumSet<Schema.Type> ROOT_TYPES = EnumSet.of(
      Schema.Type.STRING,
      Schema.Type.BYTES,
      Schema.Type.ARRAY,
      Schema.Type.INT,
      Schema.Type.LONG,
      Schema.Type.FLOAT,
      Schema.Type.DOUBLE
  );

  private final boolean fromPigAvroStorage;
  private final boolean binaryAsString;

  public AvroFlattenerMaker(final boolean fromPigAvroStorage, final boolean binaryAsString)
  {
    this.fromPigAvroStorage = fromPigAvroStorage;
    this.binaryAsString = binaryAsString;
  }

  @Override
  public Set<String> discoverRootFields(final GenericRecord obj)
  {
    return obj.getSchema()
              .getFields()
              .stream()
              .filter(field -> ROOT_TYPES.contains(field.schema().getType()))
              .map(Schema.Field::name)
              .collect(Collectors.toSet());
  }

  @Override
  public Object getRootField(final GenericRecord record, final String key)
  {
    return transformValue(record.get(key));
  }

  @Override
  public Function<GenericRecord, Object> makeJsonPathExtractor(final String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return record -> transformValue(jsonPath.read(record, JSONPATH_CONFIGURATION));
  }

  @Override
  public Function<GenericRecord, Object> makeJsonQueryExtractor(final String expr)
  {
    throw new UnsupportedOperationException("Avro + JQ not supported");
  }

  private Object transformValue(final Object field)
  {
    if (fromPigAvroStorage && field instanceof GenericData.Array) {
      return Lists.transform((List) field, item -> String.valueOf(((GenericRecord) item).get(0)));
    }
    if (field instanceof ByteBuffer) {
      if (binaryAsString) {
        return StringUtils.fromUtf8(((ByteBuffer) field).array());
      } else {
        return Arrays.toString(((ByteBuffer) field).array());
      }
    }
    if (field instanceof Utf8) {
      return field.toString();
    }
    return field;
  }
}
