/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class HttpPostEmitterStressTest
{
  private static final int N = 10_000;
  private static final Future OK_FUTURE = Futures.immediateFuture(EmitterTest.OK_RESPONSE);
  private static final ObjectMapper objectMapper = new ObjectMapper()
  {
    @Override
    public byte[] writeValueAsBytes(Object value) throws JsonProcessingException
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
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        // For this test, we don't need any batches to be dropped, i. e. "gaps" in data
        .setBatchQueueSizeLimit(1000)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, objectMapper);
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
}
