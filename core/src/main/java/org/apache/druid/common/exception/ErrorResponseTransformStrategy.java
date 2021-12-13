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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.validation.constraints.NotNull;
import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "strategy", defaultImpl = NoErrorResponseTransformStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "none", value = NoErrorResponseTransformStrategy.class),
    @JsonSubTypes.Type(name = "allowedRegex", value = AllowedRegexErrorResponseTransformStrategy.class)
})
public interface ErrorResponseTransformStrategy
{
  /**
   * For a given {@link SanitizableException} apply the transformation strategy and return the sanitized Exception
   * if the transformation stategy was applied.
   */
  default Exception transformIfNeeded(SanitizableException exception)
  {
    return exception.sanitize(getErrorMessageTransformFunction());
  }

  /**
   * Return a function for checking and transforming the error message if needed.
   * Function can return null if error message needs to be omitted or return String to be use instead.
   */
  @NotNull
  Function<String, String> getErrorMessageTransformFunction();
}
