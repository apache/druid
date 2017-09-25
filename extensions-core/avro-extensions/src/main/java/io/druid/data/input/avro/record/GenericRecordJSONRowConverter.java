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
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.parsers.Parser;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import java.util.Map;

public class GenericRecordJSONRowConverter extends GenericRecordRowConverter
{
  private final Parser<String, Object> parser;

  GenericRecordJSONRowConverter(ParseSpec parseSpec, boolean fromPigAvroStorage, boolean binaryAsString)
  {
    super(parseSpec, fromPigAvroStorage, binaryAsString);

    parser = parseSpec.makeParser();
  }

  @Override
  public MapBasedInputRow convert(GenericRecord record)
  {
    Map<String, Object> map = parser.parse(record.toString());
    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(map);
    return new MapBasedInputRow(dateTime, dimensions, map);
  }

}
