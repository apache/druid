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

import org.apache.druid.query.QueryException;

/**
 * A {@link DruidException.Failure} that serves to cover conversions from {@link QueryException}.
 *
 * When/if QueryException is completely eliminated from the code base, this compat layer should also be able to
 * be removed.  Additionally, it is the hope that nobody should actually be interacting with this class as it should
 * be an implementation detail of {@link DruidException} and not really seen outside of that.
 */
public class QueryExceptionCompat extends DruidException.Failure
{
  public static final String ERROR_CODE = "legacyQueryException";

  private final QueryException exception;

  public QueryExceptionCompat(
      QueryException exception
  )
  {
    super(ERROR_CODE);
    this.exception = exception;
  }

  @Override
  protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
  {
    return bob.forPersona(DruidException.Persona.OPERATOR)
              .ofCategory(convertFailType(exception.getFailType()))
              .build(exception, exception.getMessage())
              .withContext("host", exception.getHost())
              .withContext("errorClass", exception.getErrorClass())
              .withContext("legacyErrorCode", exception.getErrorCode());
  }

  private DruidException.Category convertFailType(QueryException.FailType failType)
  {
    switch (failType) {
      case USER_ERROR:
        return DruidException.Category.INVALID_INPUT;
      case UNAUTHORIZED:
        return DruidException.Category.UNAUTHORIZED;
      case CAPACITY_EXCEEDED:
        return DruidException.Category.CAPACITY_EXCEEDED;
      case QUERY_RUNTIME_FAILURE:
        return DruidException.Category.RUNTIME_FAILURE;
      case CANCELED:
        return DruidException.Category.CANCELED;
      case UNSUPPORTED:
        return DruidException.Category.UNSUPPORTED;
      case TIMEOUT:
        return DruidException.Category.TIMEOUT;
      case UNKNOWN:
      default:
        return DruidException.Category.UNCATEGORIZED;
    }
  }
}
