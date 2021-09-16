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

import java.util.List;
import java.util.regex.Pattern;

public class FilterableExceptionMessageAndFieldsTest
{
  @Test
  public void testApplyErrorMessageFilterWithMatchingWhitelistFilter()
  {
    String message = "test message 123";
    List<Pattern> whitelists = ImmutableList.of(Pattern.compile("acbd"), Pattern.compile("test .*"));
    String result = FilterableExceptionMessageAndFields.applyErrorMessageFilter(message, whitelists);
    Assert.assertEquals(message, result);
  }

  @Test
  public void testApplyErrorMessageFilterWithNoMatchingWhitelistFilter()
  {
    String message = "test message 123";
    List<Pattern> whitelists = ImmutableList.of(Pattern.compile("acbd"));
    String result = FilterableExceptionMessageAndFields.applyErrorMessageFilter(message, whitelists);
    Assert.assertNull(result);
  }

  @Test
  public void testApplyErrorMessageFilterWithEmptyWhitelistFilter()
  {
    String message = "test message 123";
    List<Pattern> whitelists = ImmutableList.of();
    String result = FilterableExceptionMessageAndFields.applyErrorMessageFilter(message, whitelists);
    Assert.assertNull(result);
  }
}
