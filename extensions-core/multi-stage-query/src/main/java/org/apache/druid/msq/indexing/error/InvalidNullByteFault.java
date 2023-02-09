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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName(InvalidNullByteFault.CODE)
public class InvalidNullByteFault extends BaseMSQFault
{
  static final String CODE = "InvalidNullByte";

  private final String column;

  @JsonCreator
  public InvalidNullByteFault(
      @JsonProperty("column") final String column
  )
  {
    super(CODE, "Invalid null byte in string column [%s]", column);
    this.column = column;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
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
    InvalidNullByteFault that = (InvalidNullByteFault) o;
    return Objects.equals(column, that.column);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), column);
  }
}
