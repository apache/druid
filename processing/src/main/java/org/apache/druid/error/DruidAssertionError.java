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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryException;

import java.net.HttpURLConnection;

public class DruidAssertionError extends DruidException
{
  public DruidAssertionError(
      String message
  )
  {
    this(null, message);
  }

  public DruidAssertionError(
      Throwable cause,
      String message
  )
  {
    super(
        cause,
        ErrorCode.fullCode(ErrorCode.INTERNAL_GROUP, "AssertionFailed"),
        message
    );
    this.legacyCode = QueryException.UNSUPPORTED_OPERATION_ERROR_CODE;
    this.legacyClass = UOE.class.getName();
  }

  public static DruidException forMessage(String message)
  {
    return new DruidAssertionError(SIMPLE_MESSAGE)
        .withValue(MESSAGE_KEY, message);
  }

  public static DruidException forCause(Throwable cause, String message)
  {
    return new DruidAssertionError(cause, SIMPLE_MESSAGE)
        .withValue(MESSAGE_KEY, message);
  }

  @Override
  public ErrorAudience audience()
  {
    return ErrorAudience.DRUID_DEVELOPER;
  }

  @Override
  public int httpStatus()
  {
    return HttpURLConnection.HTTP_INTERNAL_ERROR;
  }
}
