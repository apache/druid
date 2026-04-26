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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;

public class QueryInterruptedExceptionTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testErrorCode()
  {
    Assertions.assertEquals(
        "Query cancelled",
        new QueryInterruptedException(new QueryInterruptedException(new CancellationException())).getErrorCode()
    );
    Assertions.assertEquals("Query cancelled", new QueryInterruptedException(new CancellationException()).getErrorCode());
    Assertions.assertEquals("Query interrupted", new QueryInterruptedException(new InterruptedException()).getErrorCode());
    Assertions.assertEquals("Unsupported operation", new QueryInterruptedException(new UOE("Unsupported")).getErrorCode());
    Assertions.assertEquals("Unknown exception", new QueryInterruptedException(null).getErrorCode());
    Assertions.assertEquals("Unknown exception", new QueryInterruptedException(new ISE("Something bad!")).getErrorCode());
    Assertions.assertEquals(
        "Unknown exception",
        new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!"))).getErrorCode()
    );
  }

  @Test
  public void testErrorMessage()
  {
    Assertions.assertEquals(
        null,
        new QueryInterruptedException(new QueryInterruptedException(new CancellationException())).getMessage()
    );
    Assertions.assertEquals(
        null,
        new QueryInterruptedException(new CancellationException()).getMessage()
    );
    Assertions.assertEquals(
        null,
        new QueryInterruptedException(new InterruptedException()).getMessage()
    );
    Assertions.assertEquals(
        null,
        new QueryInterruptedException(null).getMessage()
    );
    Assertions.assertEquals(
        "Something bad!",
        new QueryInterruptedException(new ISE("Something bad!")).getMessage()
    );
    Assertions.assertEquals(
        "Something bad!",
        new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!"))).getMessage()
    );
  }

  @Test
  public void testErrorClass()
  {
    Assertions.assertEquals(
        "java.util.concurrent.CancellationException",
        new QueryInterruptedException(new QueryInterruptedException(new CancellationException())).getErrorClass()
    );
    Assertions.assertEquals(
        "java.util.concurrent.CancellationException",
        new QueryInterruptedException(new CancellationException()).getErrorClass()
    );
    Assertions.assertEquals(
        "java.lang.InterruptedException",
        new QueryInterruptedException(new InterruptedException()).getErrorClass()
    );
    Assertions.assertEquals(
        null,
        new QueryInterruptedException(null).getErrorClass()
    );
    Assertions.assertEquals(
        "org.apache.druid.java.util.common.ISE",
        new QueryInterruptedException(new ISE("Something bad!")).getErrorClass()
    );
    Assertions.assertEquals(
        "org.apache.druid.java.util.common.ISE",
        new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!"))).getErrorClass()
    );
  }

  @Test
  public void testHost()
  {
    Assertions.assertEquals(
        "myhost",
        new QueryInterruptedException(new QueryInterruptedException(new CancellationException(), "myhost")).getHost()
    );
  }

  @Test
  public void testSerde()
  {
    Assertions.assertEquals(
        "Query cancelled",
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new CancellationException()))).getErrorCode()
    );
    Assertions.assertEquals(
        "java.util.concurrent.CancellationException",
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new CancellationException()))).getErrorClass()
    );
    Assertions.assertEquals(
        null,
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new CancellationException()))).getMessage()
    );
    Assertions.assertEquals(
        "java.util.concurrent.CancellationException",
        roundTrip(new QueryInterruptedException(new CancellationException())).getErrorClass()
    );
    Assertions.assertEquals(
        "java.lang.InterruptedException",
        roundTrip(new QueryInterruptedException(new InterruptedException())).getErrorClass()
    );
    Assertions.assertEquals(
        null,
        roundTrip(new QueryInterruptedException(null)).getErrorClass()
    );
    Assertions.assertEquals(
        "org.apache.druid.java.util.common.ISE",
        roundTrip(new QueryInterruptedException(new ISE("Something bad!"))).getErrorClass()
    );
    Assertions.assertEquals(
        "org.apache.druid.java.util.common.ISE",
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!")))).getErrorClass()
    );
    Assertions.assertEquals(
        "Something bad!",
        roundTrip(new QueryInterruptedException(new ISE("Something bad!"))).getMessage()
    );
    Assertions.assertEquals(
        "Something bad!",
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!")))).getMessage()
    );
    Assertions.assertEquals(
        "Unknown exception",
        roundTrip(new QueryInterruptedException(new ISE("Something bad!"))).getErrorCode()
    );
    Assertions.assertEquals(
        "Unknown exception",
        roundTrip(new QueryInterruptedException(new QueryInterruptedException(new ISE("Something bad!")))).getErrorCode()
    );
  }

  @Test
  public void testToStringShouldReturnUsefulInformation()
  {
    QueryInterruptedException exception = new QueryInterruptedException(
        "error",
        "error messagez",
        "error class",
        "host"
    );
    String exceptionString = exception.toString();
    Assertions.assertTrue(exceptionString.startsWith(QueryInterruptedException.class.getSimpleName()));
    Assertions.assertTrue(exceptionString.contains("code=" + "error"));
    Assertions.assertTrue(exceptionString.contains("msg=" + "error messagez"));
    Assertions.assertTrue(exceptionString.contains("class=" + "error class"));
    Assertions.assertTrue(exceptionString.contains("host=" + "host"));

  }

  private static QueryInterruptedException roundTrip(final QueryInterruptedException e)
  {
    try {
      return MAPPER.readValue(MAPPER.writeValueAsBytes(e), QueryInterruptedException.class);
    }
    catch (Exception e2) {
      throw new RuntimeException(e2);
    }
  }
}
