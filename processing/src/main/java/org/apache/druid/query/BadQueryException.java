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

/**
 * An abstract class for all query exceptions that should return a bad request status code (400).
 *
 * See {@code BadRequestException} for non-query requests.
 */
public abstract class BadQueryException extends QueryException
{
  public static final int STATUS_CODE = 400;

  protected BadQueryException(String errorCode, String errorMessage, String errorClass)
  {
    super(errorCode, errorMessage, errorClass, null);
  }

  protected BadQueryException(String errorCode, String errorMessage, String errorClass, String host)
  {
    super(errorCode, errorMessage, errorClass, host);
  }
}
