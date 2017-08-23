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

import java.util.Objects;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.avro.AvroBytesDecoder;
import io.druid.data.input.avro.record.GenericRecordRowConverter;

/**
 *
 */
public class AvroStreamInputRowParser implements ByteBufferInputRowParser
{
  private final ParseSpec parseSpec;
  private final AvroBytesDecoder avroBytesDecoder;
  private final GenericRecordRowConverter recordConverter;

  @JsonCreator
  public AvroStreamInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("avroBytesDecoder") AvroBytesDecoder avroBytesDecoder
  )
  {
    this.parseSpec = parseSpec;
    this.avroBytesDecoder = avroBytesDecoder;
    this.recordConverter = GenericRecordRowConverter.fromParseSpec(
        parseSpec,
        false,
        false
    );
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    return this.recordConverter.convert(avroBytesDecoder.parse(input));
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AvroStreamInputRowParser that = (AvroStreamInputRowParser) o;

    return Objects.equals(avroBytesDecoder, that.avroBytesDecoder) &&
           Objects.equals(recordConverter, that.recordConverter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(avroBytesDecoder, recordConverter);
  }
}
