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

package org.apache.druid.java.util.http.client;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.time.Duration;

import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Test support for JODA time and JDK time.
 */
public class AbstractHttpClientTest
{
  private AbstractHttpClient mockAbstractHttpClient;

  @Before
  public void setUp()
  {
    mockAbstractHttpClient = partialMockBuilder(TestHttpClient.class).addMockedMethod("go",
        Request.class,
        HttpResponseHandler.class,
        org.joda.time.Duration.class).createMock();
  }

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNullDuration()
  {
    Request request = mock(Request.class);
    HttpResponseHandler handler = mock(HttpResponseHandler.class);

    expect(mockAbstractHttpClient.go(request, handler,
        (org.joda.time.Duration) null)).andReturn(null);

    replay(mockAbstractHttpClient);

    mockAbstractHttpClient.go(request, handler, (Duration) null);

    verify(mockAbstractHttpClient);
  }

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testDuration()
  {
    Request request = mock(Request.class);
    HttpResponseHandler handler = mock(HttpResponseHandler.class);
    Duration duration = Duration.ZERO;

    expect(mockAbstractHttpClient.go(request, handler,
        org.joda.time.Duration.ZERO)).andReturn(null);

    replay(mockAbstractHttpClient);

    mockAbstractHttpClient.go(request, handler, duration);

    verify(mockAbstractHttpClient);
  }

  static private abstract class TestHttpClient extends AbstractHttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        org.joda.time.Duration readTimeout) {
      // This method is mocked out
      throw new RuntimeException();
    }
  }

}
