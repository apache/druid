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

import com.amazonaws.annotation.GuardedBy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class SegmentChangeRequestHistory
{
  private static int MAX_SIZE = 1000;

  private final int maxSize;

  @GuardedBy("this")
  private final List<Holder> changes;

  @VisibleForTesting
  @GuardedBy("waitingFutures")
  final LinkedHashMap<SettableFuture, Counter> waitingFutures;

  private final ExecutorService singleThreadedExecutor;
  private final Runnable resolveWaitingFuturesRunnable;

  public SegmentChangeRequestHistory()
  {
    this(MAX_SIZE);
  }

  @VisibleForTesting
  SegmentChangeRequestHistory(int maxSize)
  {
    this.maxSize = maxSize;
    this.changes = new ArrayList<>(maxSize);

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

  private void resolveWaitingFutures()
  {
    final LinkedHashMap<SettableFuture, Counter> waitingFuturesCopy = new LinkedHashMap<>();
    synchronized (waitingFutures) {
      waitingFuturesCopy.putAll(waitingFutures);
      waitingFutures.clear();
    }

    for (Map.Entry<SettableFuture, Counter> e : waitingFuturesCopy.entrySet()) {
      try {
        e.getKey().set(getRequestsSinceWithoutWait(e.getValue()));
      } catch (Exception ex) {
        e.getKey().setException(ex);
      }
    }
  }

  public synchronized void addSegmentChangeRequests(List<DataSegmentChangeRequest> requests)
  {
    for (DataSegmentChangeRequest request : requests) {
      if (changes.size() >= maxSize) {
        changes.remove(0);
      }
      changes.add(new Holder(request, getLastCounter().inc()));
    }

    singleThreadedExecutor.execute(resolveWaitingFuturesRunnable);
  }

  public synchronized void addSegmentChangeRequest(DataSegmentChangeRequest request)
  {
    addSegmentChangeRequests(ImmutableList.of(request));
  }

  public synchronized ListenableFuture<SegmentChangeRequestsSnapshot> getRequestsSince(final Counter counter)
  {
    final SettableFuture future = new SettableFuture(waitingFutures);

    if (counter.counter < 0) {
      future.setException(new IAE("counter must be >= 0"));
      return future;
    }

    Counter lastCounter = getLastCounter();

    if (counter.counter == lastCounter.counter) {
      if (!counter.matches(lastCounter)) {
        future.setException(new IAE("counter failed to match"));
      } else {
        synchronized (waitingFutures) {
          waitingFutures.put(future, counter);
        }
      }
    } else {
      try {
        future.set(getRequestsSinceWithoutWait(counter));
      } catch (Exception ex) {
        future.setException(ex);
      }
    }

    return future;
  }

  private synchronized SegmentChangeRequestsSnapshot getRequestsSinceWithoutWait(final Counter counter)
  {
    Counter lastCounter = getLastCounter();

    if (counter.counter >= lastCounter.counter) {
      throw new IAE("counter >= last counter");
    } else if (lastCounter.counter - counter.counter >= maxSize) {
      throw new IAE("can't serve request, not enough history is kept");
    } else {
      int changeStartIndex = (int) (counter.counter + changes.size() - lastCounter.counter);

      Counter counterToMatch = counter.counter == 0 ? Counter.ZERO : changes.get(changeStartIndex - 1).counter;
      if (!counterToMatch.matches(counter)) {
        throw new IAE("counter failed to match");
      }

      List<DataSegmentChangeRequest> result = new ArrayList<>();
      for (int i = changeStartIndex; i < changes.size(); i++) {
        result.add(changes.get(i).changeRequest);
      }

      return new SegmentChangeRequestsSnapshot(changes.get(changes.size() - 1).counter, result);
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
      this.counter = counter;
      this.hash = System.currentTimeMillis();
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
      return this.counter == other.counter;// && this.hash == other.hash;
    }
  }

  private static class SettableFuture extends AbstractFuture<SegmentChangeRequestsSnapshot>
  {
    private final LinkedHashMap<SettableFuture, Counter> waitingFutures;

    private SettableFuture( LinkedHashMap<SettableFuture, Counter> waitingFutures)
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
}
