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

package org.apache.druid.query;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * This exception is for the query engine to surface when a query cannot be run. This can be due to the
 * following reasons: 1) The query is not supported yet. 2) The query is not something Druid would ever supports.
 * For these cases, the exact causes and details should also be documented in Druid user facing documents.
 *
 * As a {@link QueryException} it is expected to be serialized to a json response with a proper HTTP error code
 * ({@link #STATUS_CODE}).
 */
public class QueryUnsupportedException extends QueryException
{
  private static final String ERROR_CLASS = QueryUnsupportedException.class.getName();
  public static final String ERROR_CODE = "Unsupported query";
  public static final int STATUS_CODE = 501;

  @JsonCreator
  public QueryUnsupportedException(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host
  )
  {
    super(errorCode, errorMessage, errorClass, host);
  }

  public QueryUnsupportedException(String errorMessage)
  {
    super(ERROR_CODE, errorMessage, ERROR_CLASS, resolveHostname());
  }
}
