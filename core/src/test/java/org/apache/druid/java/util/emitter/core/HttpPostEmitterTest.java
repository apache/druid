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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
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
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(1000)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, OBJECT_MAPPER);
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
    emitter.close();

    Assert.assertEquals(2, emitter.getTotalEmittedEvents());
  }

}
