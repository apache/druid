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

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Fault to throw when the error comes from the druid native query runtime while running in the MSQ engine.
 */
@JsonTypeName(QueryRuntimeFault.CODE)
public class QueryRuntimeFault extends BaseMSQFault
{
  public static final String CODE = "QueryRuntimeError";
  @Nullable
  private final String baseErrorMessage;


  @JsonCreator
  public QueryRuntimeFault(
      @JsonProperty("errorMessage") String errorMessage,
      @Nullable @JsonProperty("baseErrorMessage") String baseErrorMessage
  )
  {
    super(CODE, errorMessage);
    this.baseErrorMessage = baseErrorMessage;
  }

  @JsonProperty
  @Nullable
  public String getBaseErrorMessage()
  {
    return baseErrorMessage;
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
    QueryRuntimeFault that = (QueryRuntimeFault) o;
    return Objects.equals(baseErrorMessage, that.baseErrorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), baseErrorMessage);
  }

}
