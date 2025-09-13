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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.AlertEvent;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * {@link ErrorResponseTransformStrategy} that modifies the error message of a {@link DruidException} based on the
 * persona. For non-user error messages, this logs the exception with a randomly generated id and returns a new exception
 * containing the id instead.
 */
public class PersonaBasedErrorTransformStrategy implements ErrorResponseTransformStrategy
{
  private static final String ERROR_WITH_ID_TEMPLATE = "Could not process the query, please contact your administrator "
                                                       + "with Error ID [%s] if the issue persists.";
  private static final EmittingLogger LOG = new EmittingLogger(PersonaBasedErrorTransformStrategy.class);

  public static final PersonaBasedErrorTransformStrategy INSTANCE = new PersonaBasedErrorTransformStrategy();

  @Override
  public Exception transformIfNeeded(DruidException druidException)
  {
    String errorId = UUID.randomUUID().toString();
    Optional<Exception> maybeMaskedException = maybeTransform(druidException, errorId);

    if (maybeMaskedException.isEmpty()) {
      return druidException;
    } else {
      LOG.makeAlert(druidException, StringUtils.format("Error ID: [%s]", errorId))
         .addData(druidException.getContext())
         .severity(AlertEvent.Severity.ANOMALY)
         .emit();
      return maybeMaskedException.get();
    }
  }

  /**
   * Transforms the {@link DruidException} if required. Returns an optional with a new Druid exception if the
   * exception was modified. Returns an empty optional if no transformation was performed.
   */
  private Optional<Exception> maybeTransform(DruidException druidException, String errorId)
  {
    if (druidException.getTargetPersona() == DruidException.Persona.USER) {
      return Optional.empty();
    } else {
      return Optional.of(DruidException.forPersona(DruidException.Persona.USER)
                                       .ofCategory(druidException.getCategory())
                                       .withErrorCode(druidException.getErrorCode())
                                       .build(StringUtils.format(ERROR_WITH_ID_TEMPLATE, errorId)));
    }
  }

  @Override
  public Function<String, String> getErrorMessageTransformFunction()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    return !(o == null || getClass() != o.getClass());
  }

  @Override
  public int hashCode()
  {
    return PersonaBasedErrorTransformStrategy.class.hashCode();
  }
}
