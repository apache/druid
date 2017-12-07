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

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.JSONPathSpec;
import io.druid.java.util.common.parsers.ObjectFlattener;
import io.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public class AvroParsers
{
  private AvroParsers()
  {
    // No instantiation.
  }

  public static ObjectFlattener<GenericRecord> makeFlattener(
      final ParseSpec parseSpec,
      final boolean fromPigAvroStorage,
      final boolean binaryAsString
  )
  {
    final JSONPathSpec flattenSpec;
    if (parseSpec != null && (parseSpec instanceof AvroParseSpec)) {
      flattenSpec = ((AvroParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }

    return ObjectFlatteners.create(flattenSpec, new AvroFlattenerMaker(fromPigAvroStorage, binaryAsString));
  }

  public static List<InputRow> parseGenericRecord(
      GenericRecord record,
      ParseSpec parseSpec,
      ObjectFlattener<GenericRecord> avroFlattener
  )
  {
    return new MapInputRowParser(parseSpec).parseBatch(avroFlattener.flatten(record));
  }
}
