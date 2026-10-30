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
import jakarta.validation.constraints.NotNull;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "strategy", defaultImpl = NoErrorResponseTransformStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "none", value = NoErrorResponseTransformStrategy.class),
    @JsonSubTypes.Type(name = "allowedRegex", value = AllowedRegexErrorResponseTransformStrategy.class),
    @JsonSubTypes.Type(name = "persona", value = PersonaBasedErrorTransformStrategy.class),
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
   * For a given {@link DruidException} apply the transformation strategy and return a sanitized Exception
   * if the transformation stategy was applied. This call does not log the exception.
   * It is the callers responsibility to do so. Returns Optional.empty() if no transformation was applied.
   * The errorId is provided to be used in the transformed Exception if needed.
   */
  default Optional<DruidException> maybeTransform(DruidException exception, Optional<String> errorId)
  {
    return Optional.empty();
  }

  /**
   * Applies {@link #maybeTransform} and returns the exception to hand back to the caller, or {@code exception} unchanged
   * if this strategy does not transform it.
   * <p>
   * A transformed exception carries only {@code errorId}, not the original message, so callers are responsible for
   * logging {@code exception} against {@code errorId}. Note also that the transformed exception carries its own category,
   * so the status code the caller sees may differ from {@code exception.getStatusCode()}.
   *
   * @param errorId id echoed back to the caller by the transformed exception; a random one is used if null
   */
  default DruidException sanitizeForClient(DruidException exception, @Nullable String errorId)
  {
    return maybeTransform(exception, Optional.ofNullable(errorId)).orElse(exception);
  }

  /**
   * Return a function for checking and transforming the error message if needed.
   * Function can return null if error message needs to be omitted or return String to be use instead.
   */
  @NotNull
  Function<String, String> getErrorMessageTransformFunction();
}
