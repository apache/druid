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

@JsonTypeName(TooManyWarningsFault.CODE)
public class TooManyWarningsFault extends BaseMSQFault
{
  static final String CODE = "TooManyWarnings";

  private final int maxWarnings;
  private final String rootErrorCode;

  @JsonCreator
  public TooManyWarningsFault(
      @JsonProperty("maxWarnings") final int maxWarnings,
      @JsonProperty("rootErrorCode") final String rootErrorCode
  )
  {
    super(CODE, "Too many warnings of type %s generated (max = %d)", rootErrorCode, maxWarnings);
    this.maxWarnings = maxWarnings;
    this.rootErrorCode = rootErrorCode;
  }

  @JsonProperty
  public int getMaxWarnings()
  {
    return maxWarnings;
  }

  @JsonProperty
  public String getRootErrorCode()
  {
    return rootErrorCode;
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
    TooManyWarningsFault that = (TooManyWarningsFault) o;
    return maxWarnings == that.maxWarnings && Objects.equals(rootErrorCode, that.rootErrorCode);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxWarnings, rootErrorCode);
  }
}
