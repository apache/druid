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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Abstract class for nested file formats such as JSON, ORC, etc.
 * It has {@link JSONPathSpec}, which is internall called {@code flattenSpec}, to flatten the nested data structure.
 */
public abstract class NestedInputFormat implements InputFormat
{
  @Nullable
  private final JSONPathSpec flattenSpec;

  protected NestedInputFormat(@Nullable JSONPathSpec flattenSpec)
  {
    this.flattenSpec = flattenSpec == null ? JSONPathSpec.DEFAULT : flattenSpec;
  }

  @Nullable
  @JsonProperty("flattenSpec")
  public JSONPathSpec getFlattenSpec()
  {
    return flattenSpec;
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
    NestedInputFormat that = (NestedInputFormat) o;
    return Objects.equals(flattenSpec, that.flattenSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(flattenSpec);
  }
}
