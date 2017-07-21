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

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class keeps a bounded list of segment updates made on the server such as adding/dropping segments.
 *
 * Clients call addSegmentChangeRequest(DataSegmentChangeRequest) or addSegmentChangeRequests(List<DataSegmentChangeRequest>)
 * to add segment updates.
 *
 * Clients call ListenableFuture<SegmentChangeRequestsSnapshot> getRequestsSince(final Counter counter) to get segment
 * updates since given counter.
 */
public class SegmentChangeRequestHistory
{
  private static int MAX_SIZE = 1000;

  private final int maxSize;

  private final CircularBuffer<Holder> changes;

  @VisibleForTesting
  final LinkedHashMap<CustomSettableFuture, Counter> waitingFutures;

  private final ExecutorService singleThreadedExecutor;
  private final Runnable resolveWaitingFuturesRunnable;

  public SegmentChangeRequestHistory()
  {
    this(MAX_SIZE);
  }

  public SegmentChangeRequestHistory(int maxSize)
  {
    this.maxSize = maxSize;
    this.changes = new CircularBuffer(maxSize);

    this.waitingFutures = new LinkedHashMap<>();

    this.resolveWaitingFuturesRunnable = new Runnable()
    {
      @Override
      public void run()
      {
        resolveWaitingFutures();
      }
    };

    this.singleThreadedExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true)
                                  .setNameFormat(
                                      "SegmentChangeRequestHistory"
                                  )
                                  .build()
    );
  }



  /**
   * Add batch of segment changes update.
   */
  public synchronized void addSegmentChangeRequests(List<DataSegmentChangeRequest> requests)
  {
    for (DataSegmentChangeRequest request : requests) {
      changes.add(new Holder(request, getLastCounter().inc()));
    }

    singleThreadedExecutor.execute(resolveWaitingFuturesRunnable);
  }

  /**
   * Add single segment change update.
   */
  public synchronized void addSegmentChangeRequest(DataSegmentChangeRequest request)
  {
    addSegmentChangeRequests(ImmutableList.of(request));
  }

  /**
   * Returns a Future that , on completion, returns list of segment updates and associated counter.
   * If there are no update since given counter then Future completion waits till an updates is provided.
   *
   * If counter is older than max number of changes maintained then SegmentChangeRequestsSnapshot is returned
   * with resetCounter set to True.
   *
   * If there were no updates to provide immediately then a future is created and returned to caller. This future
   * is added to the "waitingFutures" list and all the futures in the list get resolved as soon as a segment
   * update is provided.
   */
  public synchronized ListenableFuture<SegmentChangeRequestsSnapshot> getRequestsSince(final Counter counter)
  {
    final CustomSettableFuture future = new CustomSettableFuture(waitingFutures);

    if (counter.counter < 0) {
      future.setException(new IAE("counter[%s] must be >= 0", counter));
      return future;
    }

    Counter lastCounter = getLastCounter();

    if (counter.counter == lastCounter.counter) {
      if (!counter.matches(lastCounter)) {
        future.setException(new IAE("counter[%s] failed to match with [%s]", counter, lastCounter));
      } else {
        synchronized (waitingFutures) {
          waitingFutures.put(future, counter);
        }
      }
    } else {
      try {
        future.set(getRequestsSinceWithoutWait(counter));
      }
      catch (Exception ex) {
        future.setException(ex);
      }
    }

    return future;
  }

  private synchronized SegmentChangeRequestsSnapshot getRequestsSinceWithoutWait(final Counter counter)
  {
    Counter lastCounter = getLastCounter();

    if (counter.counter >= lastCounter.counter) {
      throw new IAE("counter[%s] >= last counter[%s]", counter, lastCounter);
    } else if (lastCounter.counter - counter.counter >= maxSize) {
      // Note: counter reset is requested when client ask for "maxSize" number of changes even if all those changes
      // are present in the history because one extra elements is needed to match the counter hash.
      return SegmentChangeRequestsSnapshot.fail(
          StringUtils.format(
              "can't serve request, not enough history is kept. given counter [%s] and current last counter [%s]",
              counter,
              lastCounter
          )
      );
    } else {
      int changeStartIndex = (int) (counter.counter + changes.size() - lastCounter.counter);

      Counter counterToMatch = counter.counter == 0 ? Counter.ZERO : changes.get(changeStartIndex - 1).counter;
      if (!counterToMatch.matches(counter)) {
        throw new IAE("counter[%s] failed to match with [%s]", counter, counterToMatch);
      }

      List<DataSegmentChangeRequest> result = new ArrayList<>();
      for (int i = changeStartIndex; i < changes.size(); i++) {
        result.add(changes.get(i).changeRequest);
      }

      return SegmentChangeRequestsSnapshot.success(changes.get(changes.size() - 1).counter, result);
    }
  }

  private void resolveWaitingFutures()
  {
    final LinkedHashMap<CustomSettableFuture, Counter> waitingFuturesCopy = new LinkedHashMap<>();
    synchronized (waitingFutures) {
      waitingFuturesCopy.putAll(waitingFutures);
      waitingFutures.clear();
    }

    for (Map.Entry<CustomSettableFuture, Counter> e : waitingFuturesCopy.entrySet()) {
      try {
        e.getKey().set(getRequestsSinceWithoutWait(e.getValue()));
      }
      catch (Exception ex) {
        e.getKey().setException(ex);
      }
    }
  }

  public synchronized Counter getLastCounter()
  {
    if (changes.size() > 0) {
      return changes.get(changes.size() - 1).counter;
    } else {
      return Counter.ZERO;
    }
  }

  private static class Holder
  {
    private final DataSegmentChangeRequest changeRequest;
    private final Counter counter;

    public Holder(DataSegmentChangeRequest changeRequest, Counter counter)
    {
      this.changeRequest = changeRequest;
      this.counter = counter;
    }
  }

  public static class Counter
  {
    public static final Counter ZERO = new Counter(0);

    private final long counter;
    private final long hash;

    public Counter(long counter)
    {
      this(counter, System.currentTimeMillis());
    }

    @JsonCreator
    public Counter(
        @JsonProperty("counter") long counter,
        @JsonProperty("hash") long hash
    )
    {
      this.counter = counter;
      this.hash = hash;
    }

    @JsonProperty
    public long getCounter()
    {
      return counter;
    }

    @JsonProperty
    public long getHash()
    {
      return hash;
    }

    public Counter inc()
    {
      return new Counter(counter + 1);
    }

    public boolean matches(Counter other)
    {
      return this.counter == other.counter && this.hash == other.hash;
    }

    @Override
    public String toString()
    {
      return "Counter{" +
             "counter=" + counter +
             ", hash=" + hash +
             '}';
    }
  }

  // Future with cancel() implementation to remove it from "waitingFutures" list
  private static class CustomSettableFuture extends AbstractFuture<SegmentChangeRequestsSnapshot>
  {
    private final LinkedHashMap<CustomSettableFuture, Counter> waitingFutures;

    private CustomSettableFuture(LinkedHashMap<CustomSettableFuture, Counter> waitingFutures)
    {
      this.waitingFutures = waitingFutures;
    }

    @Override
    public boolean set(SegmentChangeRequestsSnapshot value)
    {
      return super.set(value);
    }

    @Override
    public boolean setException(Throwable throwable)
    {
      return super.setException(throwable);
    }

    @Override
    public boolean cancel(boolean interruptIfRunning)
    {
      synchronized (waitingFutures) {
        waitingFutures.remove(this);
      }
      return true;
    }
  }

  static class CircularBuffer<E>
  {
    private final E[] buffer;

    private int start = 0;
    private int size = 0;

    CircularBuffer(int capacity)
    {
      buffer = (E[]) new Object[capacity];
    }

    void add(E item)
    {
      buffer[start++] = item;

      if (start >= buffer.length) {
        start = 0;
      }

      if (size < buffer.length) {
        size++;
      }
    }

    E get(int index)
    {
      Preconditions.checkArgument(index >= 0 && index < size, "invalid index");

      int bufferIndex = (start-size+index) % buffer.length;
      if (bufferIndex < 0) {
        bufferIndex += buffer.length;
      }
      return buffer[bufferIndex];
    }

    int size()
    {
      return size;
    }
  }
}
