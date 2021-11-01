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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.ForbiddenException;

import javax.annotation.Nonnull;

/**
 * ErrorHandler is a utilty class that is used to sanitize and log exceptions.
 */
class ErrorHandler
{
  private static final Logger LOG = new Logger(DruidMeta.class);
  private final ErrorResponseTransformStrategy errorResponseTransformStrategy;

  @Inject
  ErrorHandler(final ServerConfig serverConfig)
  {
    this.errorResponseTransformStrategy = serverConfig.getErrorResponseTransformStrategy();
  }

  /**
   * Logs a throwable at the error level and sanitizes the throwable if applicable. Will return
   * the sanitized or original throwable.
   *
   * @param error A throwable that will be logged then sanitized
   * @param <T>
   * @return The sanitized throwable
   */
  public <T extends Throwable> RuntimeException logFailureAndSanitize(T error)
  {
    return logFailureAndSanitize(error, error.getMessage());
  }

  /**
   * Logs an error message at the error level and sanitizes the throwable if applicable. Will return
   * the sanitized or original throwable.
   *
   * @param error   the throwable that will be sanitized
   * @param message the error string formate message to be logged
   * @param format  the format args for the message
   * @param <T>
   * @return A sanitized version of the throwable if applicable otherwise the original throwable
   */
  public <T extends Throwable> RuntimeException logFailureAndSanitize(T error, String message, Object... format)
  {
    logFailure(error, message, format);
    return sanitize(error);
  }

  public <T extends Throwable> T logFailure(T error, String message, Object... format) {
    LOG.error(error, message, format);
    return error;
  }

  public <T extends Throwable> T logFailure(T error) {
    logFailure(error, error.getMessage());
    return error;
  }

  public <T extends Throwable> RuntimeException sanitize(T error)
  {
    if (error instanceof QueryInterruptedException) {
      return (QueryInterruptedException) errorResponseTransformStrategy.transformIfNeeded((QueryInterruptedException) error);
    }
    if (error instanceof QueryException) {
      return (QueryException) errorResponseTransformStrategy.transformIfNeeded((QueryException) error);
    }
    if (error instanceof ForbiddenException) {
      return (ForbiddenException)  errorResponseTransformStrategy.transformIfNeeded((ForbiddenException) error);
    }
    if (error instanceof ISE) {
      return (ISE) errorResponseTransformStrategy.transformIfNeeded((ISE) error);
    }
    // catch any non explicit sanitizable exceptions
    if (error instanceof SanitizableException) {
      return new RuntimeException(errorResponseTransformStrategy.transformIfNeeded((SanitizableException) error));
    }
    // cannot check cause of the throwable because it cannot be cast back to the original's type
    // so this only checks runtime exceptions for causes
    if (error instanceof RuntimeException && error.getCause() instanceof SanitizableException) {
      // could do `throw sanitize(error);` but to avoid unnecessary going down multiple levels this is avoided here.
      return new RuntimeException(errorResponseTransformStrategy.transformIfNeeded((SanitizableException) error.getCause()));
    }
    return (QueryInterruptedException) errorResponseTransformStrategy.transformIfNeeded(QueryInterruptedException.wrapIfNeeded(
        error));
  }
  public boolean hasAffectingErrorResponseTransformStrategy() {
    return !errorResponseTransformStrategy.equals(NoErrorResponseTransformStrategy.INSTANCE);
  }
}
