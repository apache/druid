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

package org.apache.druid.msq.input.inline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.query.InlineDataSource;

import java.util.Objects;

/**
 * Input slice representing inline data. Generated from {@link InlineInputSpec}.
 */
public class InlineInputSlice implements InputSlice
{
  private final InlineDataSource dataSource;

  @JsonCreator
  public InlineInputSlice(@JsonProperty("dataSource") InlineDataSource dataSource)
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
  }

  @JsonProperty
  public InlineDataSource getDataSource()
  {
    return dataSource;
  }

  @Override
  public int fileCount()
  {
    return 1;
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
    InlineInputSlice that = (InlineInputSlice) o;
    return Objects.equals(dataSource, that.dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource);
  }

  @Override
  public String toString()
  {
    return "InlineInputSlice{" +
           "dataSource=" + dataSource +
           '}';
  }
}
