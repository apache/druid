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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryException;

public class QueryCapacityExceededException extends QueryException
{
  private static final String ERROR_CLASS = QueryCapacityExceededException.class.getName();
  public static final String ERROR_CODE = "Query capacity exceeded";
  public static final String ERROR_MESSAGE = "Total query capacity exceeded";
  public static final String ERROR_MESSAGE_TEMPLATE = "Query capacity exceeded for lane %s";
  public static final int STATUS_CODE = 429;

  public QueryCapacityExceededException()
  {
    super(ERROR_CODE, ERROR_MESSAGE, ERROR_CLASS, null);
  }

  public QueryCapacityExceededException(String lane)
  {
    super(ERROR_CODE, StringUtils.format(ERROR_MESSAGE_TEMPLATE, lane), ERROR_CLASS, null);
  }

  @JsonCreator
  public QueryCapacityExceededException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass)
  {
    super(errorCode, errorMessage, errorClass, null);
  }
}
