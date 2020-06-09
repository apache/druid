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

import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;

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
    if (parseSpec instanceof AvroParseSpec) {
      flattenSpec = ((AvroParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }

    return ObjectFlatteners.create(flattenSpec, new AvroFlattenerMaker(fromPigAvroStorage, binaryAsString));
  }

  public static List<InputRow> parseGenericRecord(
      GenericRecord record,
      MapInputRowParser mapParser,
      ObjectFlattener<GenericRecord> avroFlattener
  )
  {
    return mapParser.parseBatch(avroFlattener.flatten(record));
  }
}
