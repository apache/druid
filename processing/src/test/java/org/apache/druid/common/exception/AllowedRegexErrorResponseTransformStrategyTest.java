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

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class AllowedRegexErrorResponseTransformStrategyTest
{
  @Test
  public void testGetErrorMessageTransformFunctionWithMatchingAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of("acbd", "test .*")
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertEquals(message, result);
  }

  @Test
  public void testGetErrorMessageTransformFunctionWithNoMatchingAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of("acbd", "qwer")
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertNull(result);
  }

  @Test
  public void testGetErrorMessageTransformFunctionWithEmptyAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of()
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertNull(result);
  }

  @Test
  public void testGetErrorMessageTransformFunctionWithNullMessage()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of("acbd", "qwer")
    );
    String result = allowedRegex.getErrorMessageTransformFunction().apply(null);
    Assert.assertNull(result);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(AllowedRegexErrorResponseTransformStrategy.class)
                  .withIgnoredFields("allowedRegexPattern")
                  .usingGetClass()
                  .verify();
  }
}
