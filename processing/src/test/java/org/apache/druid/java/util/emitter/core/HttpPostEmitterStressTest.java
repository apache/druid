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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class HttpPostEmitterStressTest
{
  private static final int N = 10_000;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
  {
    @Override
    public byte[] writeValueAsBytes(Object value)
    {
      return Ints.toByteArray(((IntEvent) value).index);
    }
  };

  private final MockHttpClient httpClient = new MockHttpClient();

  @Test
  public void eventCountBased() throws InterruptedException, IOException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setFlushTimeout(BaseHttpEmittingConfig.TEST_FLUSH_TIMEOUT_MILLIS)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        // For this test, we don't need any batches to be dropped, i. e. "gaps" in data
        .setBatchQueueSizeLimit(1000)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, OBJECT_MAPPER);
    int nThreads = Runtime.getRuntime().availableProcessors() * 2;
    final List<IntList> eventsPerThread = new ArrayList<>(nThreads);
    final List<List<Batch>> eventBatchesPerThread = new ArrayList<>(nThreads);
    for (int i = 0; i < nThreads; i++) {
      eventsPerThread.add(new IntArrayList());
      eventBatchesPerThread.add(new ArrayList<Batch>());
    }
    for (int i = 0; i < N; i++) {
      eventsPerThread.get(ThreadLocalRandom.current().nextInt(nThreads)).add(i);
    }
    final BitSet emittedEvents = new BitSet(N);
    httpClient.setGoHandler(new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        ByteBuffer batch = request.getByteBufferData().slice();
        while (batch.remaining() > 0) {
          emittedEvents.set(batch.getInt());
        }
        return GoHandlers.immediateFuture(EmitterTest.okResponse());
      }
    });
    emitter.start();
    final CountDownLatch threadsCompleted = new CountDownLatch(nThreads);
    for (int i = 0; i < nThreads; i++) {
      final int threadIndex = i;
      new Thread() {
        @Override
        public void run()
        {
          IntList events = eventsPerThread.get(threadIndex);
          List<Batch> eventBatches = eventBatchesPerThread.get(threadIndex);
          IntEvent event = new IntEvent();
          for (int i = 0, eventsSize = events.size(); i < eventsSize; i++) {
            event.index = events.getInt(i);
            eventBatches.add(emitter.emitAndReturnBatch(event));
            if (i % 16 == 0) {
              try {
                Thread.sleep(10);
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }
          threadsCompleted.countDown();
        }
      }.start();
    }
    threadsCompleted.await();
    emitter.flush();
    System.out.println("Allocated buffers: " + emitter.getTotalAllocatedBuffers());
    for (int eventIndex = 0; eventIndex < N; eventIndex++) {
      if (!emittedEvents.get(eventIndex)) {
        for (int threadIndex = 0; threadIndex < eventsPerThread.size(); threadIndex++) {
          IntList threadEvents = eventsPerThread.get(threadIndex);
          int indexOfEvent = threadEvents.indexOf(eventIndex);
          if (indexOfEvent >= 0) {
            Batch batch = eventBatchesPerThread.get(threadIndex).get(indexOfEvent);
            System.err.println(batch);
            int bufferWatermark = batch.getSealedBufferWatermark();
            ByteBuffer batchBuffer = ByteBuffer.wrap(batch.buffer);
            batchBuffer.limit(bufferWatermark);
            while (batchBuffer.remaining() > 0) {
              System.err.println(batchBuffer.getInt());
            }
            break;
          }
        }
        throw new AssertionError("event " + eventIndex);
      }
    }
  }

  @Test
  public void testLargeEventsQueueLimit() throws IOException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(10)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, new ObjectMapper());

    emitter.start();

    httpClient.setGoHandler(new GoHandler() {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        return GoHandlers.immediateFuture(EmitterTest.BAD_RESPONSE);
      }
    });

    char[] chars = new char[600000];
    Arrays.fill(chars, '*');
    String bigString = new String(chars);

    Event bigEvent = ServiceMetricEvent.builder()
                                       .setFeed("bigEvents")
                                       .setDimension("test", bigString)
                                       .setMetric("metric", 10)
                                       .build("qwerty", "asdfgh");

    for (int i = 0; i < 1000; i++) {
      emitter.emit(bigEvent);
      Assert.assertTrue(emitter.getLargeEventsToEmit() <= 11);
    }

    emitter.flush();
  }

  @Test
  public void testLargeAndSmallEventsQueueLimit() throws InterruptedException, IOException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(10)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, new ObjectMapper());

    emitter.start();

    httpClient.setGoHandler(new GoHandler() {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        return GoHandlers.immediateFuture(EmitterTest.BAD_RESPONSE);
      }
    });

    char[] chars = new char[600000];
    Arrays.fill(chars, '*');
    String bigString = new String(chars);

    Event smallEvent = ServiceMetricEvent.builder()
                                       .setFeed("smallEvents")
                                       .setDimension("test", "hi")
                                       .setMetric("metric", 10)
                                       .build("qwerty", "asdfgh");

    Event bigEvent = ServiceMetricEvent.builder()
                                       .setFeed("bigEvents")
                                       .setDimension("test", bigString)
                                       .setMetric("metric", 10)
                                       .build("qwerty", "asdfgh");

    final CountDownLatch threadsCompleted = new CountDownLatch(2);
    new Thread() {
      @Override
      public void run()
      {
        for (int i = 0; i < 1000; i++) {

          emitter.emit(smallEvent);

          Assert.assertTrue(emitter.getTotalFailedBuffers() <= 10);
          Assert.assertTrue(emitter.getBuffersToEmit() <= 12);
        }
        threadsCompleted.countDown();
      }
    }.start();
    new Thread() {
      @Override
      public void run()
      {
        for (int i = 0; i < 1000; i++) {

          emitter.emit(bigEvent);

          Assert.assertTrue(emitter.getTotalFailedBuffers() <= 10);
          Assert.assertTrue(emitter.getBuffersToEmit() <= 12);
        }
        threadsCompleted.countDown();
      }
    }.start();
    threadsCompleted.await();
    emitter.flush();
  }
}
