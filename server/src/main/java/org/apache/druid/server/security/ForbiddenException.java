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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.common.exception.SanitizableException;

import java.util.function.Function;

/**
 * Throw this when a request is unauthorized and we want to send a 403 response back, Jersey exception mapper will
 * take care of sending the response.
 */
public class ForbiddenException extends RuntimeException implements SanitizableException
{
  public ForbiddenException()
  {
    super(Access.DEFAULT_ERROR_MESSAGE);
  }

  @JsonCreator
  public ForbiddenException(@JsonProperty("errorMessage") String msg)
  {
    super(msg);
  }

  @JsonProperty
  public String getErrorMessage()
  {
    return super.getMessage();
  }

  @Override
  public ForbiddenException sanitize(Function<String, String> errorMessageTransformFunction)
  {
    String transformedErrorMessage = errorMessageTransformFunction.apply(getMessage());
    if (Strings.isNullOrEmpty(transformedErrorMessage)) {
      return new ForbiddenException();
    } else {
      return new ForbiddenException(transformedErrorMessage);
    }
  }
}
