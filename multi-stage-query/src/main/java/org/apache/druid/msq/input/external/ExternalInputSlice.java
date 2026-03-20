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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Objects;

/**
 * Input slice representing external data.
 *
 * Corresponds to {@link org.apache.druid.sql.calcite.external.ExternalDataSource}.
 */
@JsonTypeName("external")
public class ExternalInputSlice implements InputSlice
{
  private final List<InputSource> inputSources;
  private final InputFormat inputFormat;
  private final RowSignature signature;

  @JsonCreator
  public ExternalInputSlice(
      @JsonProperty("inputSources") List<InputSource> inputSources,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("signature") RowSignature signature
  )
  {
    this.inputSources = inputSources;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }

  @JsonProperty
  public List<InputSource> getInputSources()
  {
    return inputSources;
  }

  @JsonProperty
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
  public int fileCount()
  {
    return inputSources.size();
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
    ExternalInputSlice that = (ExternalInputSlice) o;
    return Objects.equals(inputSources, that.inputSources)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSources, inputFormat, signature);
  }

  @Override
  public String toString()
  {
    return "ExternalInputSlice{" +
           "inputSources=" + inputSources +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
  }
}
