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

package org.apache.druid.testing.embedded;

import org.junit.jupiter.api.Assertions;

public class EmbeddedExceptionAssertions
{
  private EmbeddedExceptionAssertions()
  {
  }

  public static boolean hasMessageInChain(final Throwable throwable, final String expectedMessage)
  {
    Throwable current = throwable;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expectedMessage)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  public static void assertMessageInChain(final Throwable throwable, final String expectedMessage)
  {
    Assertions.assertTrue(
        hasMessageInChain(throwable, expectedMessage),
        () -> "Expected message [" + expectedMessage + "] in exception chain: " + throwable
    );
  }
}
