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

package org.apache.druid.data.input.parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.avro.AvroFlattenerMaker;

import java.util.ArrayList;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParquetAvroFlattenerMaker extends AvroFlattenerMaker
{
  private static Pattern jsonArrayIndexExpression = Pattern.compile(".*\\[\\d*\\]$");

  public ParquetAvroFlattenerMaker(boolean fromPigAvroStorage, boolean binaryAsString)
  {
    super(fromPigAvroStorage, binaryAsString);
  }

  @Override
  public Object getRootField(final GenericRecord record, final String key)
  {
    return finalizeConversion(super.getRootField(record, key));
  }

  @Override
  public Function<GenericRecord, Object> makeJsonPathExtractor(final String expr)
  {
    // This is sort of lame, but the avro conversion seems to lose the parquet schema repitition information, meaning
    // the only way to know to unwrap an array primitive when selected by index without also allowing arbitrarily
    // unwrapping actual GenericRecord fields that are NOT part of a list but have a single primitive field, is to
    // check for an array index expression here and explicitly further unwrap
    Matcher m = jsonArrayIndexExpression.matcher(expr);
    if (m.matches()) {
      return genericRecord ->
          unwrapArrayIndex(finalizeConversion(super.makeJsonPathExtractor(expr).apply(genericRecord)));
    }
    return genericRecord -> finalizeConversion(super.makeJsonPathExtractor(expr).apply(genericRecord));
  }

  private Object finalizeConversion(Object o)
  {
    // conversion of lists of primitives can leave 'wrapped' list primitives, which through avro conversion are
    // typed ArrayList<GenericRecord>, where the GenericRecord is of type 'RECORD' and has a single primitive field
    // Note that this can also perhaps be resolved by setting '"parquet.avro.add-list-element-records": "true"' in
    // hadoop jobProperties, but including this here to be friendly and compatible with 'simple' conversion out of the
    // box because i think it is sane default behavior
    // see https://github.com/apache/incubator-druid/issues/5433#issuecomment-388539306
    if (o instanceof ArrayList) {
      ArrayList lst = (ArrayList) o;
      if (lst.stream().allMatch(item -> {
        if (item instanceof GenericRecord) {
          GenericRecord record = (GenericRecord) item;
          Schema schema = record.getSchema();
          if (schema.getFields().size() == 1) {
            Schema itemSchema = schema.getFields().get(0).schema();
            return AvroFlattenerMaker.isPrimitive(itemSchema) || AvroFlattenerMaker.isOptionalPrimitive(itemSchema);
          }
        }
        return false;
      })) {
        return lst.stream().map(item -> unwrapArrayIndex(item)).collect(Collectors.toList());
      }
    }
    return o;
  }

  private Object unwrapArrayIndex(Object o)
  {
    if (o instanceof GenericRecord) {
      return String.valueOf(((GenericRecord) o).get(0));
    }
    return o;
  }
}
