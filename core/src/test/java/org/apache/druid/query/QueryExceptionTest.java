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

package org.apache.druid.query;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.Function;

@RunWith(MockitoJUnitRunner.class)
public class QueryExceptionTest
{
  private static final String ERROR_CODE = "error code";
  private static final String ERROR_CLASS = "error code";
  private static final String HOST = "error code";
  private static final String ERROR_MESSAGE_ORIGINAL = "aaaa";
  private static final String ERROR_MESSAGE_TRANSFORMED = "bbbb";

  @Mock
  private Function<String, String> trasformFunction;

  @Test
  public void testSanitizeWithTransformFunctionReturningNull()
  {
    Mockito.when(trasformFunction.apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL))).thenReturn(null);
    QueryException queryException = new QueryException(ERROR_CODE, ERROR_MESSAGE_ORIGINAL, ERROR_CLASS, HOST);
    QueryException actual = queryException.sanitize(trasformFunction);
    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getErrorCode(), ERROR_CODE);
    Assert.assertNull(actual.getMessage());
    Assert.assertNull(actual.getHost());
    Assert.assertNull(actual.getErrorClass());
    Mockito.verify(trasformFunction).apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL));
    Mockito.verifyNoMoreInteractions(trasformFunction);
  }

  @Test
  public void testSanitizeWithTransformFunctionReturningNewString()
  {
    Mockito.when(trasformFunction.apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL))).thenReturn(ERROR_MESSAGE_TRANSFORMED);
    QueryException queryException = new QueryException(ERROR_CODE, ERROR_MESSAGE_ORIGINAL, ERROR_CLASS, HOST);
    QueryException actual = queryException.sanitize(trasformFunction);
    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getErrorCode(), ERROR_CODE);
    Assert.assertEquals(actual.getMessage(), ERROR_MESSAGE_TRANSFORMED);
    Assert.assertNull(actual.getHost());
    Assert.assertNull(actual.getErrorClass());
    Mockito.verify(trasformFunction).apply(ArgumentMatchers.eq(ERROR_MESSAGE_ORIGINAL));
    Mockito.verifyNoMoreInteractions(trasformFunction);
  }
}
