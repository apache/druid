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

package org.apache.druid.data.input.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

/**
 * {@link org.apache.druid.data.input.InputFormat} for Thrift-encoded data. Supports binary, compact, and JSON
 * Thrift protocols (with optional Base64 encoding).
 * <p>
 * The thrift class can be provided either from the classpath or from an external jar file via {@code thriftJar}.
 */
public class ThriftInputFormat extends NestedInputFormat
{
  private final String thriftJar;
  private final String thriftClass;

  @JsonCreator
  public ThriftInputFormat(
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("thriftJar") @Nullable String thriftJar,
      @JsonProperty("thriftClass") String thriftClass
  )
  {
    super(flattenSpec);
    this.thriftJar = thriftJar;
    InvalidInput.conditionalException(thriftClass != null, "thriftClass must not be null");
    this.thriftClass = thriftClass;
  }

  @Nullable
  @JsonProperty("thriftJar")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getThriftJar()
  {
    return thriftJar;
  }

  @JsonProperty("thriftClass")
  public String getThriftClass()
  {
    return thriftClass;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new ThriftReader(inputRowSchema, source, thriftJar, thriftClass, getFlattenSpec());
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
    if (!super.equals(o)) {
      return false;
    }
    ThriftInputFormat that = (ThriftInputFormat) o;
    return Objects.equals(thriftJar, that.thriftJar) &&
           Objects.equals(thriftClass, that.thriftClass);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), thriftJar, thriftClass);
  }
}
