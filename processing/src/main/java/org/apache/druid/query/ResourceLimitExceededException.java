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
import org.apache.druid.java.util.common.StringUtils;

/**
 * Exception indicating that an operation failed because it exceeded some configured resource limit.
 *
 * This is a {@link BadQueryException} because it will likely a user's misbehavior when this exception is thrown.
 * Druid cluster operators set some set of resource limitations for some good reason. When a user query requires
 * too many resources, it will likely mean that the user query should be modified to not use excessive resources.
 */
public class ResourceLimitExceededException extends BadQueryException
{
  public static final String ERROR_CODE = "Resource limit exceeded";

  public ResourceLimitExceededException(String message, Object... arguments)
  {
    this(ERROR_CODE, StringUtils.nonStrictFormat(message, arguments), ResourceLimitExceededException.class.getName());
  }

  @JsonCreator
  private ResourceLimitExceededException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass
  )
  {
    super(errorCode, errorMessage, errorClass);
  }
}
