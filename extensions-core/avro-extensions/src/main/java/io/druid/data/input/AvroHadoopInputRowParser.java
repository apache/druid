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
package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.avro.AvroParsers;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.avro.generic.GenericRecord;

public class AvroHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final boolean fromPigAvroStorage;
  private final ObjectFlattener<GenericRecord> avroFlattener;

  @JsonCreator
  public AvroHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("fromPigAvroStorage") Boolean fromPigAvroStorage
  )
  {
    this.parseSpec = parseSpec;
    this.fromPigAvroStorage = fromPigAvroStorage == null ? false : fromPigAvroStorage;
    this.avroFlattener = AvroParsers.makeFlattener(parseSpec, this.fromPigAvroStorage, false);
  }

  @Override
  public InputRow parse(GenericRecord record)
  {
    return AvroParsers.parseGenericRecord(record, parseSpec, avroFlattener);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public boolean isFromPigAvroStorage()
  {
    return fromPigAvroStorage;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new AvroHadoopInputRowParser(parseSpec, fromPigAvroStorage);
  }
}
