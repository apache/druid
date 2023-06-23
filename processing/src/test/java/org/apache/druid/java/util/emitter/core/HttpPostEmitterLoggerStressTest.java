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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.Level;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

public class HttpPostEmitterLoggerStressTest
{
  @Rule
  public LoggerCaptureRule logCapture = new LoggerCaptureRule(HttpPostEmitter.class);

  private final MockHttpClient httpClient = new MockHttpClient();

  @Test(timeout = 20_000L)
  public void testBurstFollowedByQuietPeriod() throws InterruptedException, IOException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(5000)
        .setFlushCount(3)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(10)
        .setMinHttpTimeoutMillis(100)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, new ObjectMapper());

    emitter.start();

    httpClient.setGoHandler(new GoHandler() {
      @Override
      protected <X extends Exception> ListenableFuture<Response> go(Request request) throws X
      {
        return GoHandlers.immediateFuture(EmitterTest.okResponse());
      }
    });

    Event smallEvent = ServiceMetricEvent.builder()
                                       .setFeed("smallEvents")
                                       .setDimension("test", "hi")
                                       .build("metric", 10)
                                       .build("qwerty", "asdfgh");

    for (int i = 0; i < 1000; i++) {
      emitter.emit(smallEvent);

      Assert.assertTrue(emitter.getTotalFailedBuffers() <= 10);
      Assert.assertTrue(emitter.getBuffersToEmit() <= 12);
    }

    // by the end of this test, there should be no outstanding failed buffers

    // with a flush time of 5s, min timeout of 100ms, 20s should be
    // easily enough to get through all of the events

    while (emitter.getTotalFailedBuffers() > 0) {
      Thread.sleep(500);
    }

    // there is also no reason to have too many log events
    // refer to: https://github.com/apache/druid/issues/11279;

    long countOfTimeouts = logCapture.getLogEvents().stream()
        .filter(ev -> ev.getLevel() == Level.DEBUG)
        .filter(ev -> ev.getThrown() instanceof TimeoutException)
        .count();

    // 1000 events limit, implies we should have no more than
    // 1000 rejected send events within the expected 20sec
    // duration of the test
    long limitTimeoutEvents = 1000;

    Assert.assertTrue(
        String.format(
          Locale.getDefault(),
          "too many timeouts (%d), expect less than (%d)",
          countOfTimeouts,
          limitTimeoutEvents),
        countOfTimeouts < limitTimeoutEvents);

    emitter.close();
  }
}
