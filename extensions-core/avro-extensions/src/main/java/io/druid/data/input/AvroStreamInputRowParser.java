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
import com.google.common.base.Preconditions;
import io.druid.data.input.avro.AvroBytesDecoder;
import io.druid.data.input.avro.AvroParsers;
import io.druid.data.input.impl.ParseSpec;
import io.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class AvroStreamInputRowParser implements ByteBufferInputRowParser
{
  private final ParseSpec parseSpec;
  private final AvroBytesDecoder avroBytesDecoder;
  private final ObjectFlattener<GenericRecord> avroFlattener;

  @JsonCreator
  public AvroStreamInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("avroBytesDecoder") AvroBytesDecoder avroBytesDecoder
  )
  {
    this.parseSpec = Preconditions.checkNotNull(parseSpec, "parseSpec");
    this.avroBytesDecoder = Preconditions.checkNotNull(avroBytesDecoder, "avroBytesDecoder");
    this.avroFlattener = AvroParsers.makeFlattener(parseSpec, false, false);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    return AvroParsers.parseGenericRecord(avroBytesDecoder.parse(input), parseSpec, avroFlattener);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public AvroBytesDecoder getAvroBytesDecoder()
  {
    return avroBytesDecoder;
  }

  @Override
  public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new AvroStreamInputRowParser(
        parseSpec,
        avroBytesDecoder
    );
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AvroStreamInputRowParser that = (AvroStreamInputRowParser) o;
    return Objects.equals(parseSpec, that.parseSpec) &&
           Objects.equals(avroBytesDecoder, that.avroBytesDecoder);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec, avroBytesDecoder);
  }
}
