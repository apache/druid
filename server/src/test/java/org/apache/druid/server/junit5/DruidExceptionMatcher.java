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

package org.apache.druid.server.junit5;

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class DruidExceptionMatcher
{
  public static DruidExceptionMatcher invalidInput()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.USER,
        DruidException.Category.INVALID_INPUT,
        "invalidInput"
    );
  }

  public static DruidExceptionMatcher notFound()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.USER,
        DruidException.Category.NOT_FOUND,
        "notFound"
    );
  }

  public static DruidExceptionMatcher unsupported()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.UNSUPPORTED,
        "general"
    );
  }

  public static DruidExceptionMatcher invalidSqlInput()
  {
    return invalidInput().expectContext("sourceType", "sql");
  }

  public static DruidExceptionMatcher internalServerError()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.RUNTIME_FAILURE,
        "internalServerError"
    );
  }

  public static DruidExceptionMatcher forbidden()
  {
    return new DruidExceptionMatcher(DruidException.Persona.USER, DruidException.Category.FORBIDDEN, "general");
  }

  public static DruidExceptionMatcher conflict()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.CONFLICT,
        "general"
    );
  }

  public static DruidExceptionMatcher defensive()
  {
    return new DruidExceptionMatcher(
        DruidException.Persona.DEVELOPER,
        DruidException.Category.DEFENSIVE,
        "general"
    );
  }

  private final DruidException.Persona targetPersona;
  private final DruidException.Category category;
  private final String errorCode;
  private final Map<String, String> expectedContext = new HashMap<>();
  private String expectedMessage;
  private boolean messageContains;

  public DruidExceptionMatcher(
      DruidException.Persona targetPersona,
      DruidException.Category category,
      String errorCode
  )
  {
    this.targetPersona = targetPersona;
    this.category = category;
    this.errorCode = errorCode;
  }

  public DruidExceptionMatcher expectContext(String key, String value)
  {
    expectedContext.put(key, value);
    return this;
  }

  public DruidExceptionMatcher expectMessageIs(String message)
  {
    expectedMessage = message;
    messageContains = false;
    return this;
  }

  public DruidExceptionMatcher expectMessageContains(String message)
  {
    expectedMessage = message;
    messageContains = true;
    return this;
  }

  public boolean matches(Object item)
  {
    if (!(item instanceof DruidException)) {
      return false;
    }

    final DruidException exception = (DruidException) item;
    if (exception.getTargetPersona() != targetPersona
        || exception.getCategory() != category
        || !Objects.equals(exception.getErrorCode(), errorCode)) {
      return false;
    }

    final String actualMessage = exception.getMessage();
    if (expectedMessage != null
        && (actualMessage == null
            || (messageContains ? !actualMessage.contains(expectedMessage) : !actualMessage.equals(expectedMessage)))) {
      return false;
    }

    for (Map.Entry<String, String> contextEntry : expectedContext.entrySet()) {
      if (!Objects.equals(contextEntry.getValue(), exception.getContextValue(contextEntry.getKey()))) {
        return false;
      }
    }

    return true;
  }

  public void assertThrowsAndMatches(ThrowingSupplier supplier)
  {
    final Throwable exception = Assertions.assertThrows(Throwable.class, supplier::get);
    final DruidException druidException = Assertions.assertInstanceOf(DruidException.class, exception);
    Assertions.assertTrue(matches(druidException), () -> "Exception did not match expectations: " + druidException);
  }

  public interface ThrowingSupplier
  {
    void get();
  }
}
