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
package io.druid.data.input.avro.record;

import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Objects;

/**
 *
 */
public class GenericRecordRowConverter
{
  protected ParseSpec parseSpec;
  protected List<String> dimensions;
  protected boolean fromPigAvroStorage;
  protected boolean binaryAsString;

  protected GenericRecordRowConverter(
      ParseSpec parseSpec,
      boolean fromPigAvroStorage,
      boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.fromPigAvroStorage = fromPigAvroStorage;
    this.binaryAsString = binaryAsString;
  }

  public static GenericRecordRowConverter fromParseSpec(
      ParseSpec parseSpec,
      boolean fromPigAvroStorage,
      boolean binaryAsString
  )
  {
    return parseSpec instanceof JSONParseSpec ?
           new GenericRecordJSONRowConverter(parseSpec, fromPigAvroStorage, binaryAsString) :
           new GenericRecordRowConverter(parseSpec, fromPigAvroStorage, binaryAsString);
  }

  public MapBasedInputRow convert(GenericRecord record)
  {
    GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, fromPigAvroStorage, binaryAsString);
    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(genericRecordAsMap);
    return new MapBasedInputRow(dateTime, dimensions, genericRecordAsMap);
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

    GenericRecordRowConverter that = (GenericRecordRowConverter) o;

    return parseSpec.equals(that.parseSpec) &&
           dimensions.equals(that.dimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec, dimensions, fromPigAvroStorage);
  }
}
