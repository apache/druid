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
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryException;

/**
 * This exception is for {@link QueryResource} and SqlResource to surface when a query is cast away by
 * {@link QueryScheduler}.
 *
 * As a {@link QueryException} it is expected to be serialied to a json response, but will be mapped to
 * {@link #STATUS_CODE} instead of the default HTTP 500 status.
 */
public class QueryCapacityExceededException extends QueryException
{
  private static final String TOTAL_ERROR_MESSAGE_TEMPLATE =
      "Too many concurrent queries, total query capacity of %s exceeded. Please try your query again later.";
  private static final String LANE_ERROR_MESSAGE_TEMPLATE =
      "Too many concurrent queries for lane '%s', query capacity of %s exceeded. Please try your query again later.";
  private static final String ERROR_CLASS = QueryCapacityExceededException.class.getName();
  public static final String ERROR_CODE = "Query capacity exceeded";
  public static final int STATUS_CODE = 429;

  public QueryCapacityExceededException(int capacity)
  {
    super(ERROR_CODE, makeTotalErrorMessage(capacity), ERROR_CLASS, null);
  }

  public QueryCapacityExceededException(String lane, int capacity)
  {
    super(ERROR_CODE, makeLaneErrorMessage(lane, capacity), ERROR_CLASS, null);
  }

  @JsonCreator
  public QueryCapacityExceededException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass)
  {
    super(errorCode, errorMessage, errorClass, null);
  }

  @VisibleForTesting
  public static String makeTotalErrorMessage(int capacity)
  {
    return StringUtils.format(TOTAL_ERROR_MESSAGE_TEMPLATE, capacity);
  }

  @VisibleForTesting
  public static String makeLaneErrorMessage(String lane, int capacity)
  {
    return StringUtils.format(LANE_ERROR_MESSAGE_TEMPLATE, lane, capacity);
  }
}
