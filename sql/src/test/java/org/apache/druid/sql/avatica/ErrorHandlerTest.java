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

package org.apache.druid.sql.avatica;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.exception.AllowedRegexErrorResponseTransformStrategy;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ErrorHandlerTest
{

  @Test
  public void testErrorHandlerSanitizesErrorAsExpected()
  {
    ServerConfig serverConfig = Mockito.mock(ServerConfig.class);
    AllowedRegexErrorResponseTransformStrategy emptyAllowedRegexErrorResponseTransformStrategy = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of());

    Mockito.when(serverConfig.getErrorResponseTransformStrategy())
           .thenReturn(emptyAllowedRegexErrorResponseTransformStrategy);
    ErrorHandler errorHandler = new ErrorHandler(serverConfig);
    QueryException input = new QueryException("error", "error message", "error class", "host");

    RuntimeException output = errorHandler.sanitize(input);
    Assert.assertNull(output.getMessage());
  }

  @Test
  public void testErrorHandlerDefaultErrorResponseTransformStrategySanitizesErrorAsExpected()
  {
    ServerConfig serverConfig = new ServerConfig();
    ErrorHandler errorHandler = new ErrorHandler(serverConfig);
    QueryInterruptedException input = new QueryInterruptedException("error", "error messagez", "error class", "host");

    RuntimeException output = errorHandler.sanitize(input);
    Assert.assertEquals("error messagez", output.getMessage());
  }

  @Test
  public void testErrorHandlerHasAffectingErrorResponseTransformStrategyReturnsTrueWhenNotUsingNoErrorResponseTransformStrategy()
  {
    ServerConfig serverConfig = Mockito.mock(ServerConfig.class);
    AllowedRegexErrorResponseTransformStrategy emptyAllowedRegexErrorResponseTransformStrategy = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of());

    Mockito.when(serverConfig.getErrorResponseTransformStrategy())
           .thenReturn(emptyAllowedRegexErrorResponseTransformStrategy);
    ErrorHandler errorHandler = new ErrorHandler(serverConfig);
    Assert.assertTrue(errorHandler.hasAffectingErrorResponseTransformStrategy());
  }

  @Test
  public void testErrorHandlerHasAffectingErrorResponseTransformStrategyReturnsFalseWhenUsingNoErrorResponseTransformStrategy()
  {
    ServerConfig serverConfig = new ServerConfig();
    ErrorHandler errorHandler = new ErrorHandler(serverConfig);
    Assert.assertFalse(errorHandler.hasAffectingErrorResponseTransformStrategy());
  }

  @Test
  public void testErrorHandlerHandlesNonSanitizableExceptionCorrectly()
  {
    ServerConfig serverConfig = Mockito.mock(ServerConfig.class);
    AllowedRegexErrorResponseTransformStrategy emptyAllowedRegexErrorResponseTransformStrategy = new AllowedRegexErrorResponseTransformStrategy(
        ImmutableList.of());

    Mockito.when(serverConfig.getErrorResponseTransformStrategy())
           .thenReturn(emptyAllowedRegexErrorResponseTransformStrategy);
    ErrorHandler errorHandler = new ErrorHandler(serverConfig);

    Exception input = new Exception("message");
    RuntimeException output = errorHandler.sanitize(input);
    Assert.assertEquals(null, output.getMessage());
  }
}
