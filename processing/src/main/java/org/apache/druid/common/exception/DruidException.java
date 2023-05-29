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

package org.apache.druid.common.exception;

/**
 * A generic exception thrown by Druid.
 */
public class DruidException extends RuntimeException
{
  public static final int DEFAULT_HTTP_CODE = 500;
  public static final int HTTP_CODE_BAD_REQUEST = 400;

  private static final boolean DEFAULT_CAN_RETRY = false;

  private final int responseCode;
  private final boolean isTransient;

  public DruidException(String message)
  {
    this(message, DEFAULT_HTTP_CODE, null, DEFAULT_CAN_RETRY);
  }

  public DruidException(String message, Throwable cause)
  {
    this(message, DEFAULT_HTTP_CODE, cause, DEFAULT_CAN_RETRY);
  }

  public DruidException(String message, int responseCode)
  {
    this(message, responseCode, null, DEFAULT_CAN_RETRY);
  }

  public DruidException(String message, int responseCode, Throwable cause)
  {
    this(message, responseCode, cause, false);
  }

  public DruidException(String message, int responseCode, Throwable cause, boolean isTransient)
  {
    super(message, cause);
    this.responseCode = responseCode;
    this.isTransient = isTransient;
  }

  public int getResponseCode()
  {
    return responseCode;
  }

  /**
   * Returns true if this is a transient exception and might go away if the
   * operation is retried. All DruidException are non-transient by default
   * unless specified otherwise.
   */
  public boolean isTransient()
  {
    return isTransient;
  }
}
