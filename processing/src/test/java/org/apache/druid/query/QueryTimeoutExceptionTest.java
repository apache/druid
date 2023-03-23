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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class QueryTimeoutExceptionTest
{
  @Test
  public void testSerde() throws IOException
  {
    // We re-create the configuration from DefaultObjectMapper here because this is in `core` and
    // DefaultObjectMapper is in `processing`.  Hopefully that distinction disappears at some point
    // in time, but it exists today and moving things one way or the other quickly turns into just
    // chunking it all together.
    final ObjectMapper mapper = new ObjectMapper()
    {
      {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        // See https://github.com/FasterXML/jackson-databind/issues/170
        // configure(MapperFeature.AUTO_DETECT_CREATORS, false);
        configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        configure(MapperFeature.AUTO_DETECT_SETTERS, false);
        configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false);
        configure(SerializationFeature.INDENT_OUTPUT, false);
        configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);
      }
    };

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
        "Query did not complete within configured timeout period. You can increase " +
            "query timeout or tune the performance of query.",
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
