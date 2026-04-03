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
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.emitter.service.UnitEvent;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class HttpPostEmitterTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
  {
    @Override
    public byte[] writeValueAsBytes(Object value)
    {
      return Ints.toByteArray(((IntEvent) value).index);
    }
  };

  private final MockHttpClient httpClient = new MockHttpClient();

  @Before
  public void setup()
  {
    httpClient.setGoHandler(new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        return GoHandlers.immediateFuture(EmitterTest.okResponse());
      }
    });
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testRecoveryEmitAndReturnBatch()
      throws InterruptedException, IOException, NoSuchFieldException, IllegalAccessException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setFlushTimeout(BaseHttpEmittingConfig.TEST_FLUSH_TIMEOUT_MILLIS)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(1000)
        .build();
    try (final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, OBJECT_MAPPER)) {
      emitter.start();

      // emit first event
      emitter.emitAndReturnBatch(new IntEvent());
      Thread.sleep(1000L);

      // get concurrentBatch reference and set value to lon as if it would fail while
      // HttpPostEmitter#onSealExclusive method invocation.
      Field concurrentBatch = emitter.getClass().getDeclaredField("concurrentBatch");
      concurrentBatch.setAccessible(true);
      ((AtomicReference<Object>) concurrentBatch.get(emitter)).getAndSet(1L);
      // something terrible happened previously so that batch has to recover

      // emit second event
      emitter.emitAndReturnBatch(new IntEvent());

      emitter.flush();

      Assert.assertEquals(2, emitter.getTotalEmittedEvents());
    }
  }

  @Test
  public void testFilteringAllowsConfiguredMetricAndNonMetricEvent() throws Exception
  {
    final HttpEmitterConfig.Builder builder = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setFlushTimeout(BaseHttpEmittingConfig.TEST_FLUSH_TIMEOUT_MILLIS)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(1000);
    builder.setShouldFilterMetrics(true);

    final HttpEmitterConfig config = builder.build();
    final List<String> payloads = new ArrayList<>();
    httpClient.setGoHandler(
        new GoHandler()
        {
          @Override
          protected ListenableFuture<Response> go(final Request request)
          {
            payloads.add(StandardCharsets.UTF_8.decode(request.getByteBufferData().slice()).toString());
            return GoHandlers.immediateFuture(EmitterTest.okResponse());
          }
        }
    );

    try (final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, new ObjectMapper())) {
      emitter.start();
      emitter.emit(ServiceMetricEvent.builder().setMetric("query/time", 100).build("test", "localhost"));
      emitter.emit(ServiceMetricEvent.builder().setMetric("some/unlisted/metric", 200).build("test", "localhost"));
      emitter.emit(new UnitEvent("alerts", 1));
      emitter.flush();

      final String payload = String.join("", payloads);
      Assert.assertTrue(payload.contains("\"metric\":\"query/time\""));
      Assert.assertTrue(payload.contains("\"feed\":\"alerts\""));
      Assert.assertFalse(payload.contains("\"metric\":\"some/unlisted/metric\""));
      Assert.assertEquals(2, emitter.getTotalEmittedEvents());
    }
  }
}
