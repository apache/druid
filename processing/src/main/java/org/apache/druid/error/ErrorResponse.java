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

package org.apache.druid.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Union of the {@link org.apache.druid.query.QueryException} and
 * {@link DruidException} fields. Used in tests to deserialize errors which may
 * be in either format.
 */
public class ErrorResponse
{
  private final String msg;
  private final String code;
  private final String errorClass;
  private final String host;
  private final DruidException.ErrorType type;
  private Map<String, String> context;

  @JsonCreator
  public ErrorResponse(
      @JsonProperty("error") @Nullable String errorCode,
      @JsonProperty("errorMessage") @Nullable String errorMessage,
      @JsonProperty("errorClass") @Nullable String errorClass,
      @JsonProperty("host") @Nullable String host,
      @JsonProperty("type") @Nullable DruidException.ErrorType type,
      @JsonProperty("context") @Nullable Map<String, String> context
  )
  {
    this.msg = errorMessage;
    this.code = errorCode;
    this.errorClass = errorClass;
    this.host = host;
    this.type = type;
    this.context = context;
  }

  @Nullable
  @JsonProperty("error")
  @JsonInclude(Include.NON_NULL)
  public String getErrorCode()
  {
    return code;
  }

  @JsonProperty("errorMessage")
  public String getMessage()
  {
    return msg;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getErrorClass()
  {
    return errorClass;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public Map<String, String> getContext()
  {
    return context;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DruidException.ErrorType getType()
  {
    return type;
  }
}
