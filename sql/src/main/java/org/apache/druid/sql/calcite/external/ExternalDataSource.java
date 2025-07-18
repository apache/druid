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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.query.LeafDataSource;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents external data for INSERT queries. Only used by the SQL layer, not by the query stack.
 *
 * Includes an {@link InputSource} and {@link InputFormat}, plus a {@link RowSignature} so the SQL planner has
 * the type information necessary to validate and plan the query.
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 */
@JsonTypeName("external")
public class ExternalDataSource extends LeafDataSource
{
  private final InputSource inputSource;
  private final InputFormat inputFormat;
  private final RowSignature signature;

  @JsonCreator
  public ExternalDataSource(
      @JsonProperty("inputSource") final InputSource inputSource,
      @JsonProperty("inputFormat") final InputFormat inputFormat,
      @JsonProperty("signature") final RowSignature signature
  )
  {
    this.inputSource = Preconditions.checkNotNull(inputSource, "inputSource");
    this.inputFormat = Preconditions.checkNotNull(inputFormat, "inputFormat");
    this.signature = Preconditions.checkNotNull(signature, "signature");
  }

  @JsonProperty
  public InputSource getInputSource()
  {
    return inputSource;
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
  public Set<String> getTableNames()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isProcessable()
  {
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
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
    ExternalDataSource that = (ExternalDataSource) o;
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
    return "ExternalDataSource{" +
           "inputSource=" + inputSource +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
  }
}
