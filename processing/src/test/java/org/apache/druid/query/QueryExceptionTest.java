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

import org.apache.druid.query.QueryException.FailType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class QueryExceptionTest
{
  private static final String ERROR_CODE = "error code";
  private static final String ERROR_CLASS = "error code";
  private static final String HOST = "error code";
  private static final String ERROR_MESSAGE_ORIGINAL = "aaaa";
  private static final String ERROR_MESSAGE_TRANSFORMED = "bbbb";

  @Test
  public void testSanitizeWithTransformFunctionReturningNull()
  {
    QueryException queryException = new QueryException(ERROR_CODE, ERROR_MESSAGE_ORIGINAL, ERROR_CLASS, HOST);

    AtomicLong callCount = new AtomicLong(0);
    QueryException actual = queryException.sanitize(s -> {
      callCount.incrementAndGet();
      Assert.assertEquals(ERROR_MESSAGE_ORIGINAL, s);
      return null;
    });

    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getErrorCode(), ERROR_CODE);
    Assert.assertNull(actual.getMessage());
    Assert.assertNull(actual.getHost());
    Assert.assertNull(actual.getErrorClass());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testSanitizeWithTransformFunctionReturningNewString()
  {
    QueryException queryException = new QueryException(ERROR_CODE, ERROR_MESSAGE_ORIGINAL, ERROR_CLASS, HOST);

    AtomicLong callCount = new AtomicLong(0);
    QueryException actual = queryException.sanitize(s -> {
      callCount.incrementAndGet();
      Assert.assertEquals(ERROR_MESSAGE_ORIGINAL, s);
      return ERROR_MESSAGE_TRANSFORMED;
    });

    Assert.assertNotNull(actual);
    Assert.assertEquals(actual.getErrorCode(), ERROR_CODE);
    Assert.assertEquals(actual.getMessage(), ERROR_MESSAGE_TRANSFORMED);
    Assert.assertNull(actual.getHost());
    Assert.assertNull(actual.getErrorClass());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testSanity()
  {
    expectFailTypeForCode(FailType.UNKNOWN, null);
    expectFailTypeForCode(FailType.UNKNOWN, "Nobody knows me.");
    expectFailTypeForCode(FailType.QUERY_RUNTIME_FAILURE, QueryException.UNKNOWN_EXCEPTION_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.JSON_PARSE_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.BAD_QUERY_CONTEXT_ERROR_CODE);
    expectFailTypeForCode(FailType.CAPACITY_EXCEEDED, QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE);
    expectFailTypeForCode(FailType.QUERY_RUNTIME_FAILURE, QueryException.QUERY_INTERRUPTED_ERROR_CODE);
    expectFailTypeForCode(FailType.CANCELED, QueryException.QUERY_CANCELED_ERROR_CODE);
    expectFailTypeForCode(FailType.UNAUTHORIZED, QueryException.UNAUTHORIZED_ERROR_CODE);
    expectFailTypeForCode(FailType.QUERY_RUNTIME_FAILURE, QueryException.UNSUPPORTED_OPERATION_ERROR_CODE);
    expectFailTypeForCode(FailType.QUERY_RUNTIME_FAILURE, QueryException.TRUNCATED_RESPONSE_CONTEXT_ERROR_CODE);
    expectFailTypeForCode(FailType.TIMEOUT, QueryException.QUERY_TIMEOUT_ERROR_CODE);
    expectFailTypeForCode(FailType.UNSUPPORTED, QueryException.QUERY_UNSUPPORTED_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.SQL_PARSE_FAILED_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.PLAN_VALIDATION_FAILED_ERROR_CODE);
    expectFailTypeForCode(FailType.USER_ERROR, QueryException.SQL_QUERY_UNSUPPORTED_ERROR_CODE);
  }

  /**
   * This test exists primarily to get branch coverage of the null check on the QueryException constructor.
   * The validations done in this test are not actually intended to be set-in-stone or anything.
   */
  @Test
  public void testCanConstructWithoutThrowable()
  {
    QueryException exception = new QueryException(
        (Throwable) null,
        QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
        "java.lang.Exception",
        "test"
    );

    Assert.assertEquals(QueryException.UNKNOWN_EXCEPTION_ERROR_CODE, exception.getErrorCode());
    Assert.assertNull(exception.getMessage());
  }

  @Test
  public void testToStringReturnsUsefulInformation()
  {
    QueryException queryException = new QueryException(ERROR_CODE, ERROR_MESSAGE_ORIGINAL, ERROR_CLASS, HOST);
    String exceptionToString = queryException.toString();
    Assert.assertTrue(exceptionToString.startsWith(QueryException.class.getSimpleName()));
    Assert.assertTrue(exceptionToString.contains("msg=" + ERROR_MESSAGE_ORIGINAL));
    Assert.assertTrue(exceptionToString.contains("code=" + ERROR_CODE));
    Assert.assertTrue(exceptionToString.contains("class=" + ERROR_CLASS));
    Assert.assertTrue(exceptionToString.contains("host=" + HOST));
  }

  private void expectFailTypeForCode(FailType expected, String code)
  {
    QueryException exception = new QueryException(new Exception(), code, "java.lang.Exception", "test");

    Assert.assertEquals(code, expected, exception.getFailType());
  }
}
