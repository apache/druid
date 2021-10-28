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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Objects;

public class AvroStreamInputFormat extends NestedInputFormat
{
  private final boolean binaryAsString;
  private final boolean extractUnionsByType;

  private final AvroBytesDecoder avroBytesDecoder;

  @JsonCreator
  public AvroStreamInputFormat(
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("avroBytesDecoder") AvroBytesDecoder avroBytesDecoder,
      @JsonProperty("binaryAsString") @Nullable Boolean binaryAsString,
      @JsonProperty("extractUnionsByType") @Nullable Boolean extractUnionsByType
  )
  {
    super(flattenSpec);
    this.avroBytesDecoder = avroBytesDecoder;
    this.binaryAsString = binaryAsString != null && binaryAsString;
    this.extractUnionsByType = extractUnionsByType != null && extractUnionsByType;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @JsonProperty
  public AvroBytesDecoder getAvroBytesDecoder()
  {
    return avroBytesDecoder;
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
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new AvroStreamReader(
        inputRowSchema,
        source,
        avroBytesDecoder,
        getFlattenSpec(),
        binaryAsString,
        extractUnionsByType
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
    final AvroStreamInputFormat that = (AvroStreamInputFormat) o;
    return Objects.equals(getFlattenSpec(), that.getFlattenSpec()) &&
           Objects.equals(avroBytesDecoder, that.avroBytesDecoder) &&
           Objects.equals(binaryAsString, that.binaryAsString) &&
           Objects.equals(extractUnionsByType, that.extractUnionsByType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getFlattenSpec(), avroBytesDecoder, binaryAsString, extractUnionsByType);
  }
}
