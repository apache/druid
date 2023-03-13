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

/**
 * Fault to throw when the error cames from the druid native query stack while running in the MSQ engine .
 */
@JsonTypeName(QueryStackFault.CODE)
public class QueryStackFault extends BaseMSQFault
{
  public static final String CODE = "QueryStackError";
  private final String baseErrorMessage;


  @JsonCreator
  public QueryStackFault(
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("baseErrorMessage") String baseErrorMessage
  )
  {
    super(CODE, errorMessage);
    this.baseErrorMessage = baseErrorMessage;
  }

  @JsonProperty
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
    QueryStackFault that = (QueryStackFault) o;
    return Objects.equals(baseErrorMessage, that.baseErrorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), baseErrorMessage);
  }

}
