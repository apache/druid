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

package org.apache.druid.extensions.watermarking.storage.memory;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import org.apache.druid.extensions.watermarking.WatermarkCollectorConfig;
import org.apache.druid.extensions.watermarking.storage.WatermarkStore;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;


public class MemoryWatermarkStore implements WatermarkStore
{
  private static final Logger log = new Logger(MemoryWatermarkStore.class);

  private final ConcurrentMap<String, Map<String, MemoryWatermarkState>> memStoreSet;
  private final Striped<ReadWriteLock> lock;
  private final WatermarkCollectorConfig config;

  @Inject
  public MemoryWatermarkStore(
      ConcurrentHashMap<String, Map<String, MemoryWatermarkState>> store,
      WatermarkCollectorConfig config
  )
  {
    this.memStoreSet = store;
    this.config = config;
    this.lock = Striped.readWriteLock(config.getNumThreads());
    log.info("Configured in memory timeline watermark metadata storage");
  }

  @Override
  public void update(String datasource, String type, DateTime timestamp)
  {
    Lock lock = this.lock.get(datasource).writeLock();
    lock.lock();
    try {
      log.debug("Updating %s timeline watermark %s to %s", datasource, type, timestamp.toString());
      memStoreSet.putIfAbsent(datasource, new HashMap<>());
      Map<String, MemoryWatermarkState> ds =
          memStoreSet.get(datasource);

      ds.compute(type, (s, memoryWatermarkState) -> {
        if (memoryWatermarkState != null) {
          memoryWatermarkState.update(timestamp);
          return memoryWatermarkState;
        } else {
          return new MemoryWatermarkState(timestamp, config.getMaxHistoryResults());
        }
      });
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void rollback(String datasource, String type, DateTime timestamp)
  {
    Lock lock = this.lock.get(datasource).writeLock();
    lock.lock();
    try {
      log.debug("Rolling back %s timeline watermark %s to %s", datasource, type, timestamp.toString());
      memStoreSet.putIfAbsent(datasource, new HashMap<>());
      Map<String, MemoryWatermarkState> ds =
          memStoreSet.get(datasource);

      ds.compute(type, (s, memoryWatermarkState) -> {
        if (memoryWatermarkState != null) {
          memoryWatermarkState.rollback(timestamp);
          return memoryWatermarkState;
        } else {
          return new MemoryWatermarkState(timestamp, config.getMaxHistoryResults());
        }
      });
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void purgeHistory(String datasource, String type, DateTime timestamp)
  {
    Lock lock = this.lock.get(datasource).writeLock();
    lock.lock();
    try {
      log.debug("Purging history for %s timeline watermark %s older than %s", datasource, type, timestamp.toString());
      memStoreSet.putIfAbsent(datasource, new HashMap<>());
      Map<String, MemoryWatermarkState> ds =
          memStoreSet.get(datasource);

      ds.computeIfPresent(type, (s, memoryWatermarkState) -> {
        memoryWatermarkState.purge(timestamp);
        return memoryWatermarkState;
      });
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public DateTime getValue(String datasource, String type)
  {
    log.debug("Fetching %s timeline watermark for %s", datasource, type);

    Map<String, MemoryWatermarkState> ds = memStoreSet.get(datasource);
    if (ds != null) {
      MemoryWatermarkState entry = ds.get(type);
      Lock lock = this.lock.get(datasource).readLock();
      lock.lock();
      try {
        if (entry != null) {
          return entry.getLatest();
        }
      }
      finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public Collection<String> getDatasources()
  {
    return memStoreSet.keySet();
  }

  @Override
  public Map<String, DateTime> getValues(String datasource)
  {
    log.debug("Fetching %s timeline watermark", datasource);
    Map<String, MemoryWatermarkState> ds = memStoreSet.get(datasource);
    if (ds != null) {
      Lock lock = this.lock.get(datasource).readLock();
      lock.lock();
      try {
        return ds.entrySet()
                 .stream()
                 .collect(Collectors.toMap(
                     Map.Entry::getKey,
                     e -> e.getValue().getLatest()
                 ));
      }
      finally {
        lock.unlock();
      }
    } else {
      return null;
    }
  }

  @Override
  public List<Pair<DateTime, DateTime>> getValueHistory(String datasource, String type, Interval range)
  {
    log.debug("Fetching %s timeline watermark history for %s between %s and %s",
              datasource, type, range.getStart().toString(), range.getEnd().toString()
    );
    Map<String, MemoryWatermarkState> ds = memStoreSet.get(datasource);
    if (ds != null) {
      Lock lock = this.lock.get(datasource).readLock();
      MemoryWatermarkState entry = ds.get(type);
      lock.lock();
      if (entry != null) {
        try {
          return ds.get(type)
                   .getHistory()
                   .stream()
                   .sequential()
                   .filter(dateTimeDateTimePair ->
                               range.getStart().equals(dateTimeDateTimePair.lhs) ||
                               range.contains(dateTimeDateTimePair.lhs)
                   ).collect(Collectors.toList());
        }
        finally {
          lock.unlock();
        }
      }
    }
    return null;
  }

  @Override
  public void initialize()
  {
  }

  public class MemoryWatermarkState
  {
    private final Object lock = new Object();
    private DateTime latest;
    private EvictingQueue<Pair<DateTime, DateTime>> history;

    MemoryWatermarkState(DateTime latest, int maxSize)
    {
      this.latest = latest;
      this.history = EvictingQueue.create(maxSize);
      history.add(new Pair<>(latest, DateTimes.nowUtc()));
    }

    MemoryWatermarkState update(DateTime timestamp)
    {
      synchronized (lock) {
        if (latest.isBefore(timestamp)) {
          latest = timestamp;
          history.add(new Pair<>(latest, DateTimes.nowUtc()));
        }
        return this;
      }
    }

    MemoryWatermarkState rollback(DateTime timestamp)
    {
      synchronized (lock) {
        latest = timestamp;
        history.add(new Pair<>(latest, DateTimes.nowUtc()));
        return this;
      }
    }

    MemoryWatermarkState purge(DateTime timestamp)
    {
      synchronized (lock) {
        List<Pair<DateTime, DateTime>> legit =
            history
                .stream()
                .sequential()
                .filter(dateTimeDateTimePair -> dateTimeDateTimePair.rhs.isAfter(timestamp))
                .collect(Collectors.toList());
        history.clear();
        history.addAll(legit);
        return this;
      }
    }

    DateTime getLatest()
    {
      return latest;
    }

    List<Pair<DateTime, DateTime>> getHistory()
    {
      synchronized (lock) {
        return ImmutableList.copyOf(history);
      }
    }
  }
}
