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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Objects;

public class ThriftInputFormat extends NestedInputFormat
{
  private final String jarPath;
  private final String thriftClassName;

  @JsonCreator
  public ThriftInputFormat(
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("thriftJar") String jarPath,
      @JsonProperty("thriftClass") String thriftClassName
  )
  {
    super(flattenSpec);
    this.jarPath = jarPath;
    this.thriftClassName = thriftClassName;
    Preconditions.checkNotNull(thriftClassName, "thrift class name");
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @JsonProperty
  public String getThriftJar()
  {
    return jarPath;
  }

  @JsonProperty
  public String getThriftClass()
  {
    return thriftClassName;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new ThriftReader(
        inputRowSchema,
        source,
        jarPath,
        thriftClassName,
        getFlattenSpec()
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
    final ThriftInputFormat that = (ThriftInputFormat) o;
    return Objects.equals(getFlattenSpec(), that.getFlattenSpec()) &&
        Objects.equals(jarPath, that.jarPath) &&
        Objects.equals(thriftClassName, that.thriftClassName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(jarPath, thriftClassName, getFlattenSpec());
  }

}
