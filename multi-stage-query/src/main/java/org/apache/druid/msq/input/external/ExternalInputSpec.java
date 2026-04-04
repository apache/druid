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

package org.apache.druid.msq.input.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.segment.column.RowSignature;

import java.util.Objects;

@JsonTypeName("external")
public class ExternalInputSpec implements InputSpec
{
  private final InputSource inputSource;
  private final InputFormat inputFormat;
  private final RowSignature signature;

  @JsonCreator
  public ExternalInputSpec(
      @JsonProperty("inputSource") InputSource inputSource,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("signature") RowSignature signature
  )
  {
    this.inputSource = Preconditions.checkNotNull(inputSource, "inputSource");
    this.inputFormat = inputFormat;
    this.signature = Preconditions.checkNotNull(signature, "signature");

    if (inputSource.needsFormat()) {
      Preconditions.checkNotNull(inputFormat, "inputFormat");
    }
  }

  @JsonProperty
  public InputSource getInputSource()
  {
    return inputSource;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public RowSignature getSignature()
  {
    return signature;
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
    ExternalInputSpec that = (ExternalInputSpec) o;
    return Objects.equals(inputSource, that.inputSource)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSource, inputFormat, signature);
  }

  @Override
  public String toString()
  {
    return "ExternalInputSpec{" +
           "inputSources=" + inputSource +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
  }
}
