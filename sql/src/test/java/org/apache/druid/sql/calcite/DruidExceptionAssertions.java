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

package org.apache.druid.sql.calcite;

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import javax.annotation.Nullable;

public class DruidExceptionAssertions
{
  private final DruidException.Persona persona;
  private final DruidException.Category category;
  private final String errorCode;
  @Nullable
  private String expectedMessage;
  @Nullable
  private String expectedMessagePart;
  @Nullable
  private String contextKey;
  @Nullable
  private String contextValue;

  public DruidExceptionAssertions(
      final DruidException.Persona persona,
      final DruidException.Category category,
      final String errorCode
  )
  {
    this.persona = persona;
    this.category = category;
    this.errorCode = errorCode;
  }

  public static DruidExceptionAssertions invalidInput()
  {
    return new DruidExceptionAssertions(
        DruidException.Persona.USER,
        DruidException.Category.INVALID_INPUT,
        "invalidInput"
    );
  }

  public static DruidExceptionAssertions invalidSqlInput()
  {
    return invalidInput().expectContext("sourceType", "sql");
  }

  public DruidExceptionAssertions expectMessageIs(final String message)
  {
    expectedMessage = message;
    return this;
  }

  public DruidExceptionAssertions expectMessageContains(final String messagePart)
  {
    expectedMessagePart = messagePart;
    return this;
  }

  public DruidExceptionAssertions expectContext(final String key, final String value)
  {
    contextKey = key;
    contextValue = value;
    return this;
  }

  public void assertThrowsAndMatches(final Executable executable)
  {
    assertMatches(Assertions.assertThrows(DruidException.class, executable));
  }

  public void assertMatches(final DruidException exception)
  {
    Assertions.assertAll(
        () -> Assertions.assertEquals(persona, exception.getTargetPersona()),
        () -> Assertions.assertEquals(category, exception.getCategory()),
        () -> Assertions.assertEquals(errorCode, exception.getErrorCode()),
        () -> {
          if (expectedMessage != null) {
            Assertions.assertEquals(expectedMessage, exception.getMessage());
          }
        },
        () -> {
          if (expectedMessagePart != null) {
            Assertions.assertTrue(exception.getMessage().contains(expectedMessagePart), exception.getMessage());
          }
        },
        () -> {
          if (contextKey != null) {
            Assertions.assertEquals(contextValue, exception.getContext().get(contextKey));
          }
        }
    );
  }
}
