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

package org.apache.druid.sql.avatica;

import com.google.inject.Inject;
import org.apache.druid.common.exception.ErrorResponseTransformStrategy;
import org.apache.druid.common.exception.NoErrorResponseTransformStrategy;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.ForbiddenException;


/**
 * ErrorHandler is a utility class that is used to sanitize exceptions.
 */
class ErrorHandler
{
  private final ErrorResponseTransformStrategy errorResponseTransformStrategy;

  @Inject
  ErrorHandler(final ServerConfig serverConfig)
  {
    this.errorResponseTransformStrategy = serverConfig.getErrorResponseTransformStrategy();
  }

  /**
   * Sanitizes a Throwable. If it's a runtime exception and it's cause is sanitizable it will sanitize that cause and
   * return that cause as a sanitized RuntimeException.  This will do best effort to keep original exception type. If
   * it's a checked exception that will be turned into a QueryInterruptedException.
   *
   * @param error The Throwable to be sanitized
   * @param <T>   Any class that extends Throwable
   * @return The sanitized Throwable
   */
  public <T extends Throwable> RuntimeException sanitize(T error)
  {
    if (error instanceof QueryException) {
      return (QueryException) errorResponseTransformStrategy.transformIfNeeded((QueryException) error);
    }
    if (error instanceof ForbiddenException) {
      return (ForbiddenException) errorResponseTransformStrategy.transformIfNeeded((ForbiddenException) error);
    }
    // Should map BasicSecurityAuthenticationException also, but the class is not
    // visible here.
    if (error instanceof ISE) {
      return (ISE) errorResponseTransformStrategy.transformIfNeeded((ISE) error);
    }
    if (error instanceof UOE) {
      return (UOE) errorResponseTransformStrategy.transformIfNeeded((UOE) error);
    }
    // catch any non explicit sanitizable exceptions
    if (error instanceof SanitizableException) {
      return new RuntimeException(errorResponseTransformStrategy.transformIfNeeded((SanitizableException) error));
    }
    // cannot check cause of the throwable because it cannot be cast back to the original's type
    // so this only checks runtime exceptions for causes
    if (error instanceof RuntimeException && error.getCause() instanceof SanitizableException) {
      // could do `throw sanitize(error);` but just sanitizing immediately avoids unnecessary going down multiple levels
      return new RuntimeException(errorResponseTransformStrategy.transformIfNeeded((SanitizableException) error.getCause()));
    }
    QueryInterruptedException wrappedError = QueryInterruptedException.wrapIfNeeded(error);
    return (QueryException) errorResponseTransformStrategy.transformIfNeeded(wrappedError);
  }

  /**
   * Check to see if something needs to be sanitized.
   * <p>
   * Done by checking to see if the ErrorResponse is different than a NoOp Error response transform strategy.
   *
   * @return a boolean that returns true if error handler has an error response strategy other than the NoOp error
   * response strategy
   */
  public boolean hasAffectingErrorResponseTransformStrategy()
  {
    return !errorResponseTransformStrategy.equals(NoErrorResponseTransformStrategy.INSTANCE);
  }
}
