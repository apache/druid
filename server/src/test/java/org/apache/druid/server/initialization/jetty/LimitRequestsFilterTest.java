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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.CountDownLatch;

/**
 */
public class LimitRequestsFilterTest
{
  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    LimitRequestsFilter filter = new LimitRequestsFilter(2);

    CountDownLatch latch1 = createAndStartRequestThread(
        filter,
        EasyMock.createStrictMock(ServletRequest.class),
        EasyMock.createStrictMock(HttpServletResponse.class)
    );

    CountDownLatch latch2 = createAndStartRequestThread(
        filter,
        EasyMock.createStrictMock(ServletRequest.class),
        EasyMock.createStrictMock(HttpServletResponse.class)
    );

    while (filter.getActiveRequestsCount() != 2) {
      Thread.sleep(100);
    }

    //now further requests should fail
    HttpServletResponse resp = EasyMock.createMock(HttpServletResponse.class);
    resp.sendError(429, "Too Many Requests");
    EasyMock.expectLastCall().times(2);
    EasyMock.replay(resp);

    filter.doFilter(
        EasyMock.createStrictMock(ServletRequest.class),
        resp,
        EasyMock.createStrictMock(FilterChain.class)
    );

    filter.doFilter(
        EasyMock.createStrictMock(ServletRequest.class),
        resp,
        EasyMock.createStrictMock(FilterChain.class)
    );

    EasyMock.verify(resp);

    //release one of the pending requests
    latch1.countDown();

    while (filter.getActiveRequestsCount() != 1) {
      Thread.sleep(100);
    }

    //now requests should go through
    FilterChain chain = EasyMock.createMock(FilterChain.class);
    chain.doFilter(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);
    EasyMock.replay(chain);

    filter.doFilter(
        EasyMock.createStrictMock(ServletRequest.class),
        EasyMock.createStrictMock(HttpServletResponse.class),
        chain
    );

    filter.doFilter(
        EasyMock.createStrictMock(ServletRequest.class),
        EasyMock.createStrictMock(HttpServletResponse.class),
        chain
    );

    EasyMock.verify(chain);

    latch2.countDown();

    while (filter.getActiveRequestsCount() != 0) {
      Thread.sleep(100);
    }
  }

  private CountDownLatch createAndStartRequestThread(LimitRequestsFilter filter, ServletRequest req, HttpServletResponse resp)
  {
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(
        () -> {
          try {
            filter.doFilter(req, resp, new TestFilterChain(latch));
          }
          catch (Exception e) {
            throw new ISE(e, "exception");
          }
        }
    ).start();
    return latch;
  }

  private static class TestFilterChain implements FilterChain
  {
    private final CountDownLatch latch;

    TestFilterChain(CountDownLatch latch)
    {
      this.latch = latch;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response)
    {
      try {
        latch.await();
      }
      catch (InterruptedException ex) {
        throw new ISE(ex, "exception");
      }
    }
  }
}
