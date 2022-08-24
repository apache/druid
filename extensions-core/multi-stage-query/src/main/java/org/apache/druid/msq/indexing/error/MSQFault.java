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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 * Error code for multi-stage queries.
 *
 * See {@link MSQErrorReport#getFaultFromException} for a mapping of exception type to error code.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "errorCode")
public interface MSQFault
{
  String getErrorCode();

  @Nullable
  String getErrorMessage();

  default String getCodeWithMessage()
  {
    final String message = getErrorMessage();

    if (message != null && !message.isEmpty()) {
      return getErrorCode() + ": " + message;
    } else {
      return getErrorCode();
    }
  }
}
