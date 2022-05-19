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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class QueryTimeoutExceptionTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    QueryTimeoutException timeoutException = mapper.readValue(
        mapper.writeValueAsBytes(new QueryTimeoutException()),
        QueryTimeoutException.class
    );
    QueryTimeoutException timeoutExceptionWithMsg = mapper.readValue(mapper.writeValueAsBytes(new QueryTimeoutException(
        "Another query timeout")), QueryTimeoutException.class);

    Assert.assertEquals(
        "Query timeout",
        timeoutException.getErrorCode()
    );
    Assert.assertEquals(
        "Query Timed Out!",
        timeoutException.getMessage()
    );
    Assert.assertEquals(
        "Another query timeout",
        timeoutExceptionWithMsg.getMessage()
    );
    Assert.assertEquals(
        "org.apache.druid.query.QueryTimeoutException",
        timeoutExceptionWithMsg.getErrorClass()
    );
  }

  @Test
  public void testExceptionHost()
  {
    Assert.assertEquals(
        "timeouthost",
        new QueryTimeoutException("Timed out", "timeouthost").getHost()
    );
  }
}
