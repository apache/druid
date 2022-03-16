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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;

/**
 * Class used by {@link RowSignature} for serialization.
 *
 * Package-private since it is not intended to be used outside that narrow use case. In other cases where passing
 * around information about column types is important, use {@link ColumnType} instead.
 */
class ColumnSignature
{
  private final String name;

  @Nullable
  private final ColumnType type;

  @JsonCreator
  ColumnSignature(
      @JsonProperty("name") String name,
      @JsonProperty("type") @Nullable ColumnType type
  )
  {
    this.name = name;
    this.type = type;

    // Name must be nonnull, but type can be null (if the type is unknown)
    if (name == null || name.isEmpty()) {
      throw new IAE(name, "Column name must be non-empty");
    }
  }

  @JsonProperty("name")
  String name()
  {
    return name;
  }

  @Nullable
  @JsonProperty("type")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  ColumnType type()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "ColumnSignature{" +
           "name='" + name + '\'' +
           ", type=" + type +
           '}';
  }
}
