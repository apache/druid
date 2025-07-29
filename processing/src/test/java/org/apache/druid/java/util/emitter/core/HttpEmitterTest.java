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
import com.google.common.primitives.Ints;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class HttpEmitterTest
{
  private final MockHttpClient httpClient = new MockHttpClient();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
  {
    @Override
    public byte[] writeValueAsBytes(Object value)
    {
      return Ints.toByteArray(((IntEvent) value).index);
    }
  };

  private final AtomicLong timeoutUsed = new AtomicLong();

  @Before
  public void setup()
  {
    timeoutUsed.set(-1L);

    httpClient.setGoHandler(new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        Duration timeout = request.getRequestTimeout();
        timeoutUsed.set(timeout.toMillis());
        return GoHandlers.immediateFuture(EmitterTest.okResponse());
      }
    });
  }

  @Test
  public void timeoutEmptyQueue() throws Exception
  {
    // In HttpPostEmitter, when batch is empty, the timeout is lastBatchFillTimeMillis * config.httpTimeoutAllowanceFactor, and lastBatchFillTimeMillis is at least 1.
    double timeoutAllowanceFactor = 2.0d;
    final HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setHttpTimeoutAllowanceFactor((float) timeoutAllowanceFactor)
        .setFlushTimeout(BaseHttpEmittingConfig.TEST_FLUSH_TIMEOUT_MILLIS)
        .build();
    Field lastBatchFillTimeMillis = HttpPostEmitter.class.getDeclaredField("lastBatchFillTimeMillis");
    lastBatchFillTimeMillis.setAccessible(true);
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, OBJECT_MAPPER);

    long startMs = System.currentTimeMillis();
    emitter.start();
    emitter.emitAndReturnBatch(new IntEvent());
    emitter.flush();

    // sometimes System.currentTimeMillis() - startMs might be 0, so we need to use Math.max(1, System.currentTimeMillis() - startMs)
    long fillTimeMs = Math.max(1, System.currentTimeMillis() - startMs);
    Assume.assumeTrue(fillTimeMs >= (Long) lastBatchFillTimeMillis.get(emitter));
    MatcherAssert.assertThat(
        (double) timeoutUsed.get(),
        Matchers.lessThanOrEqualTo(fillTimeMs * timeoutAllowanceFactor)
    );

    startMs = System.currentTimeMillis();
    final Batch batch = emitter.emitAndReturnBatch(new IntEvent());
    Thread.sleep(1000);
    batch.seal();
    emitter.flush();
    // sometimes System.currentTimeMillis() - startMs might be 0, so we need to use Math.max(1, System.currentTimeMillis() - startMs)
    fillTimeMs = Math.max(1, System.currentTimeMillis() - startMs);
    Assume.assumeTrue(fillTimeMs >= (Long) lastBatchFillTimeMillis.get(emitter));
    MatcherAssert.assertThat(
        (double) timeoutUsed.get(),
        Matchers.lessThanOrEqualTo(fillTimeMs * timeoutAllowanceFactor)
    );
  }
}
