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

package org.apache.druid.segment;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Buildable dictionary for some comparable type. Values are unsorted, or rather sorted in the order which they are
 * added. A {@link SortedDimensionDictionary} can be constructed with a mapping of ids from this dictionary to the
 * sorted dictionary with the {@link #sort()} method.
 * <p>
 * Concrete implementations of this dictionary must be thread-safe.
 */
public abstract class DimensionDictionary<T extends Comparable<T>>
{
  public static final int ABSENT_VALUE_ID = -1;
  private final Class<T> cls;

  @Nullable
  private T minValue = null;
  @Nullable
  private T maxValue = null;
  private volatile int idForNull = ABSENT_VALUE_ID;

  private final AtomicLong sizeInBytes = new AtomicLong(0L);
  private final Object2IntMap<T> valueToId = new Object2IntOpenHashMap<>();

  private final List<T> idToValue = new ArrayList<>();
  private final StampedLock lock;

  public DimensionDictionary(Class<T> cls)
  {
    this.cls = cls;
    this.lock = new StampedLock();
    valueToId.defaultReturnValue(ABSENT_VALUE_ID);
  }

  public int getId(@Nullable T value)
  {
    if (value == null) {
      return idForNull;
    }

    long stamp = lock.readLock();
    try {
      return valueToId.getInt(value);
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  @Nullable
  public T getValue(int id)
  {
    if (id == idForNull) {
      return null;
    }

    // optimistic read
    long stamp = lock.tryOptimisticRead();
    T output = idToValue.get(id);
    if (lock.validate(stamp)) {
      return output;
    }

    // classic lock
    stamp = lock.readLock();
    try {
      return idToValue.get(id);
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public T[] getValues(int[] ids)
  {
    T[] values = (T[]) Array.newInstance(cls, ids.length);

    long stamp = lock.readLock();
    try {
      for (int i = 0; i < ids.length; i++) {
        values[i] = idToValue.get(ids[i]);
      }
      return values;
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public int size()
  {
    // using idToValue rather than valueToId because the valueToId doesn't account null value, if it is present.

    // optimistic read
    long stamp = lock.tryOptimisticRead();
    int size = idToValue.size();
    if (lock.validate(stamp)) {
      return size;
    }

    // classic lock
    stamp = lock.readLock();
    try {
      return idToValue.size();
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Gets the current size of this dictionary in bytes.
   *
   * @throws IllegalStateException if size computation is disabled.
   */
  public long sizeInBytes()
  {
    if (!computeOnHeapSize()) {
      throw new IllegalStateException("On-heap size computation is disabled");
    }

    return sizeInBytes.get();
  }

  public int add(@Nullable T originalValue)
  {
    if (originalValue == null) {
      return addNull();
    }

    long stamp = lock.tryReadLock();
    if (stamp != 0) {
      try {
        int existing = valueToId.getInt(originalValue);
        if (existing >= 0) {
          return existing;
        }
      }
      finally {
        lock.unlockRead(stamp);
      }
    }

    long extraSize = 0;
    if (computeOnHeapSize()) {
      // Add size of new dim value and 2 references (valueToId and idToValue)
      extraSize = estimateSizeOfValue(originalValue) + 2L * Long.BYTES;
    }

    stamp = lock.writeLock();
    try {
      final int index = idToValue.size();
      final int prev = valueToId.putIfAbsent(originalValue, index);
      if (prev >= 0) {
        return prev;
      }

      idToValue.add(originalValue);
      sizeInBytes.addAndGet(extraSize);

      minValue = minValue == null || minValue.compareTo(originalValue) > 0 ? originalValue : minValue;
      maxValue = maxValue == null || maxValue.compareTo(originalValue) < 0 ? originalValue : maxValue;
      return index;
    }
    finally {
      lock.unlockWrite(stamp);
    }
  }

  private int addNull()
  {
    if (idForNull != ABSENT_VALUE_ID) {
      return idForNull;
    }

    long stamp = lock.writeLock();
    try {
      // check, in case it was changed by another thread
      if (idForNull == ABSENT_VALUE_ID) {
        idForNull = idToValue.size();
        idToValue.add(null);
      }
      return idForNull;
    }
    finally {
      lock.unlockWrite(stamp);
    }
  }

  public T getMinValue()
  {
    // optimistic read
    long stamp = lock.tryOptimisticRead();
    T output = minValue;
    if (lock.validate(stamp)) {
      return output;
    }

    // classic lock
    stamp = lock.readLock();
    try {
      return minValue;
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public T getMaxValue()
  {
    // optimistic read
    long stamp = lock.tryOptimisticRead();
    T output = maxValue;
    if (lock.validate(stamp)) {
      return output;
    }

    // classic lock
    stamp = lock.readLock();
    try {
      return maxValue;
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public int getIdForNull()
  {
    return idForNull;
  }

  public SortedDimensionDictionary<T> sort()
  {
    long stamp = lock.readLock();
    try {
      return new SortedDimensionDictionary<>(idToValue, idToValue.size());
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Estimates the size of the dimension value in bytes.
   * <p>
   * This method is called when adding a new dimension value to the lookup only
   * if {@link #computeOnHeapSize()} returns true.
   */
  public abstract long estimateSizeOfValue(T value);

  /**
   * Whether on-heap size of this dictionary should be computed.
   */
  public abstract boolean computeOnHeapSize();

}
