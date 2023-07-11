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

package org.apache.druid.server.security;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.Function;

@RunWith(MockitoJUnitRunner.class)
public class ForbiddenExceptionTest
{
  private static final String ERROR_MESSAGE_ORIGINAL = "aaaa";
  private static final String ERROR_MESSAGE_TRANSFORMED = "bbbb";

  @Mock
  private Function<String, String> trasformFunction;

  @Test
  public void testSanitizeWithTransformFunctionReturningNull()
  {
    Mockito.when(trasformFunction.apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL))).thenReturn(null);
    ForbiddenException forbiddenException = new ForbiddenException(ERROR_MESSAGE_ORIGINAL);
    ForbiddenException actual = forbiddenException.sanitize(trasformFunction);
    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getMessage(), Access.DEFAULT_ERROR_MESSAGE);
    Mockito.verify(trasformFunction).apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL));
    Mockito.verifyNoMoreInteractions(trasformFunction);
  }

  @Test
  public void testSanitizeWithTransformFunctionReturningNewString()
  {
    Mockito.when(trasformFunction.apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL))).thenReturn(ERROR_MESSAGE_TRANSFORMED);
    ForbiddenException forbiddenException = new ForbiddenException(ERROR_MESSAGE_ORIGINAL);
    ForbiddenException actual = forbiddenException.sanitize(trasformFunction);
    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getMessage(), ERROR_MESSAGE_TRANSFORMED);
    Mockito.verify(trasformFunction).apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL));
    Mockito.verifyNoMoreInteractions(trasformFunction);
  }

  // Silly, but required to get the code coverage tests to pass.
  @Test
  public void testAccess()
  {
    Access access = new Access(false);
    Assert.assertFalse(access.isAllowed());
    Assert.assertEquals("", access.getMessage());
    Assert.assertEquals("Allowed:false, Message:", access.toString());
    Assert.assertEquals(Access.DEFAULT_ERROR_MESSAGE, access.toMessage());

    access = new Access(true);
    Assert.assertTrue(access.isAllowed());
    Assert.assertEquals("", access.getMessage());
    Assert.assertEquals("Allowed:true, Message:", access.toString());
    Assert.assertEquals("Authorized", access.toMessage());

    access = new Access(false, "oops");
    Assert.assertFalse(access.isAllowed());
    Assert.assertEquals("oops", access.getMessage());
    Assert.assertEquals("Allowed:false, Message:oops", access.toString());
    Assert.assertEquals("Allowed:false, Message:oops", access.toMessage());
  }
}
