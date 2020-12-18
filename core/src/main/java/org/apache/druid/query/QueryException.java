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
 * Base serializable error response
 *
 * QueryResource and SqlResource are expected to emit the JSON form of this object when errors happen.
 */
public class QueryException extends RuntimeException
{
  private final String errorCode;
  private final String errorClass;
  private final String host;

  public QueryException(Throwable cause, String errorCode, String errorClass, String host)
  {
    super(cause == null ? null : cause.getMessage(), cause);
    this.errorCode = errorCode;
    this.errorClass = errorClass;
    this.host = host;
  }

  @JsonCreator
  public QueryException(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host
  )
  {
    super(errorMessage);
    this.errorCode = errorCode;
    this.errorClass = errorClass;
    this.host = host;
  }

  @Nullable
  @JsonProperty("error")
  public String getErrorCode()
  {
    return errorCode;
  }

  @JsonProperty("errorMessage")
  @Override
  public String getMessage()
  {
    return super.getMessage();
  }

  @JsonProperty
  public String getErrorClass()
  {
    return errorClass;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }
}
