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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.avro.AvroParsers;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;

import java.util.List;
import java.util.Objects;

public class AvroHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final boolean fromPigAvroStorage;
  private final boolean binaryAsString;
  private final boolean extractUnionsByType;
  private final ObjectFlattener<GenericRecord> avroFlattener;
  private final MapInputRowParser mapParser;

  @JsonCreator
  public AvroHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("fromPigAvroStorage") Boolean fromPigAvroStorage,
      @JsonProperty("binaryAsString") Boolean binaryAsString,
      @JsonProperty("extractUnionsByType") Boolean extractUnionsByType
  )
  {
    this.parseSpec = parseSpec;
    this.fromPigAvroStorage = fromPigAvroStorage != null && fromPigAvroStorage;
    this.binaryAsString = binaryAsString != null && binaryAsString;
    this.extractUnionsByType = extractUnionsByType != null && extractUnionsByType;
    this.avroFlattener = AvroParsers.makeFlattener(parseSpec, this.fromPigAvroStorage, this.binaryAsString, this.extractUnionsByType);
    this.mapParser = new MapInputRowParser(parseSpec);
  }

  @Override
  public List<InputRow> parseBatch(GenericRecord record)
  {
    return AvroParsers.parseGenericRecord(record, mapParser, avroFlattener);
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

  @JsonProperty
  public Boolean getBinaryAsString()
  {
    return binaryAsString;
  }

  @JsonProperty
  public Boolean isExtractUnionsByType()
  {
    return extractUnionsByType;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new AvroHadoopInputRowParser(parseSpec, fromPigAvroStorage, binaryAsString, extractUnionsByType);
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
    AvroHadoopInputRowParser that = (AvroHadoopInputRowParser) o;
    return fromPigAvroStorage == that.fromPigAvroStorage
           && binaryAsString == that.binaryAsString
           && extractUnionsByType == that.extractUnionsByType
           && Objects.equals(parseSpec, that.parseSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec, fromPigAvroStorage, binaryAsString, extractUnionsByType);
  }

  @Override
  public String toString()
  {
    return "AvroHadoopInputRowParser{" +
           "parseSpec=" + parseSpec +
           ", fromPigAvroStorage=" + fromPigAvroStorage +
           ", binaryAsString=" + binaryAsString +
           ", extractUnionsByType=" + extractUnionsByType +
           '}';
  }
}
