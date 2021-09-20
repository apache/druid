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
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class AllowedRegexErrorResponseTransformStrategyTest
{
  @Test
  public void testgetErrorMessageTransformFunctionWithMatchingAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of(Pattern.compile("acbd"), Pattern.compile("test .*"))
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertEquals(message, result);
  }

  @Test
  public void testgetErrorMessageTransformFunctionWithNoMatchingAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of(Pattern.compile("acbd"), Pattern.compile("qwer"))
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertNull(result);
  }

  @Test
  public void testgetErrorMessageTransformFunctionWithEmptyAllowedRegexFilter()
  {
    AllowedRegexErrorResponseTransformStrategy allowedRegex = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of()
    );
    String message = "test message 123";
    String result = allowedRegex.getErrorMessageTransformFunction().apply(message);
    Assert.assertNull(result);
  }
}
