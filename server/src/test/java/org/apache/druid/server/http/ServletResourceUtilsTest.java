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

package org.apache.druid.server.http;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;

public class ServletResourceUtilsTest
{

  @Test
  public void testSanitizeException()
  {
    final String message = "some message";
    Assert.assertEquals(message, ServletResourceUtils.sanitizeException(new Throwable(message)).get("error"));
    Assert.assertEquals("null", ServletResourceUtils.sanitizeException(null).get("error"));
    Assert.assertEquals(message, ServletResourceUtils.sanitizeException(new Throwable()
    {
      @Override
      public String toString()
      {
        return message;
      }
    }).get("error"));
  }

  @Test
  public void testBuildErrorReponseFrom()
  {
    DruidException exception = InvalidInput.exception("Invalid value of [%s]", "inputKey");
    Response response = ServletResourceUtils.buildErrorResponseFrom(exception);
    Assert.assertEquals(exception.getStatusCode(), response.getStatus());

    Object entity = response.getEntity();
    Assert.assertTrue(entity instanceof ErrorResponse);
    MatcherAssert.assertThat(
        ((ErrorResponse) entity).getUnderlyingException(),
        DruidExceptionMatcher.invalidInput().expectMessageIs("Invalid value of [inputKey]")
    );
  }

  @Test
  public void test_getDefaultValueIfCauseIs404ElseThrow()
  {
    final StringFullResponseHolder notFoundResponseHolder = new StringFullResponseHolder(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
        StandardCharsets.UTF_8
    );
    Assert.assertEquals(
        "abc",
        ServletResourceUtils.getDefaultValueIfCauseIs404ElseThrow(
            new ISE(new HttpResponseException(notFoundResponseHolder), ""),
            () -> "abc"
        )
    );

    final StringFullResponseHolder badRequestResponseHolder = new StringFullResponseHolder(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST),
        StandardCharsets.UTF_8
    );
    Assert.assertThrows(
        RuntimeException.class,
        () -> ServletResourceUtils.getDefaultValueIfCauseIs404ElseThrow(
            new ISE(new HttpResponseException(badRequestResponseHolder), ""),
            () -> "abc"
        )
    );

    Assert.assertThrows(
        RuntimeException.class,
        () -> ServletResourceUtils.getDefaultValueIfCauseIs404ElseThrow(new ISE(""), () -> "abc")
    );
  }
}
