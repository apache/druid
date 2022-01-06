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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

/**
 * Buildable dictionary for some comparable type. Values are unsorted, or rather sorted in the order which they are
 * added. A {@link SortedDimensionDictionary} can be constructed with a mapping of ids from this dictionary to the
 * sorted dictionary with the {@link #sort()} method.
 *
 * This dictionary is thread-safe.
 */
public class DimensionDictionary<T extends Comparable<T>>
{
  public static final int ABSENT_VALUE_ID = -1;

  @Nullable
  private T minValue = null;
  @Nullable
  private T maxValue = null;
  private volatile int idForNull = ABSENT_VALUE_ID;

  private final Object2IntMap<T> valueToId = new Object2IntOpenHashMap<>();

  private final List<T> idToValue = new ArrayList<>();
  private final StampedLock lock;

  public DimensionDictionary()
  {
    this.lock = new StampedLock();
    valueToId.defaultReturnValue(ABSENT_VALUE_ID);
  }

  public int getId(@Nullable T value)
  {
    long stamp = lock.readLock();
    try {
      if (value == null) {
        return idForNull;
      }
      return valueToId.getInt(value);
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  @Nullable
  public T getValue(int id)
  {
    long stamp = lock.readLock();
    try {
      if (id == idForNull) {
        return null;
      }
      return idToValue.get(id);
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public int size()
  {
    long stamp = lock.readLock();
    try {
      // using idToValue rather than valueToId because the valueToId doesn't account null value, if it is present.
      return idToValue.size();
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public AddResult add(@Nullable T originalValue)
  {
    if (originalValue == null) {
      return addNull();
    }

    // attempt a read for existing value
    long stamp = lock.readLock();
    try {
      int prev = valueToId.getInt(originalValue);
      if (prev >= 0) {
        return AddResult.existingAt(prev);
      }
    }
    finally {
      lock.unlockRead(stamp);
    }

    // could not get an existing value, send a write
    stamp = lock.writeLock();
    try {
      // check again for existing, in case
      int prev = valueToId.getInt(originalValue);
      if (prev >= 0) {
        return AddResult.existingAt(prev);
      }

      // otherwise, add it
      final int index = idToValue.size();
      valueToId.put(originalValue, index);
      idToValue.add(originalValue);
      minValue = minValue == null || minValue.compareTo(originalValue) > 0 ? originalValue : minValue;
      maxValue = maxValue == null || maxValue.compareTo(originalValue) < 0 ? originalValue : maxValue;
      return AddResult.addedAt(index);
    }
    finally {
      lock.unlockWrite(stamp);
    }
  }

  /**
   * Add the null value.
   *
   * @return
   */
  protected AddResult addNull()
  {
    long stamp;
    AddResult result;

    // try until we can complete an optimistic read
    do {
      stamp = lock.tryOptimisticRead();
      result = (idForNull == ABSENT_VALUE_ID) ? null : AddResult.existingAt(idForNull);
    } while (!lock.validate(stamp));

    // optimistic read found an ID
    if (result != null) {
      return result;
    }

    // optimistic read found no ID, set it up
    stamp = lock.writeLock();
    try {
      if (idForNull == ABSENT_VALUE_ID) {
        idForNull = idToValue.size();
        idToValue.add(null);
        return AddResult.addedAt(idForNull);
      } else {
        return AddResult.existingAt(idForNull);
      }
    }
    finally {
      lock.unlockWrite(stamp);
    }
  }

  public T getMinValue()
  {
    long stamp;
    T result;
    do {
      stamp = lock.tryOptimisticRead();
      result = minValue;
    } while (!lock.validate(stamp));

    return result;
  }

  public T getMaxValue()
  {
    long stamp;
    T result;
    do {
      stamp = lock.tryOptimisticRead();
      result = maxValue;
    } while (!lock.validate(stamp));

    return result;
  }

  public int getIdForNull()
  {
    return idForNull;
  }

  public SortedDimensionDictionary<T> sort()
  {
    long stamp = lock.readLock();
    try {
      return new SortedDimensionDictionary<T>(idToValue, idToValue.size());
    }
    finally {
      lock.unlockRead(stamp);
    }
  }

  public static class AddResult
  {
    final int index;
    final boolean wasAdded;

    AddResult(final int index, final boolean wasAdded)
    {
      this.index = index;
      this.wasAdded = wasAdded;
    }

    static AddResult existingAt(final int index)
    {
      return new AddResult(index, false);
    }

    static AddResult addedAt(final int index)
    {
      return new AddResult(index, true);
    }

    boolean wasAdded()
    {
      return wasAdded;
    }

    int getIndex()
    {
      return index;
    }
  }

}
