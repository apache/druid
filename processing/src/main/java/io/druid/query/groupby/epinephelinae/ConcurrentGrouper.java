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

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grouper based around a set of underlying {@link SpillingGrouper} instances. Thread-safe.
 *
 * The passed-in buffer is cut up into concurrencyHint slices, and each slice is passed to a different underlying
 * grouper. Access to each slice is separately synchronized. As long as the result set fits in memory, keys are
 * partitioned between buffers based on their hash, and multiple threads can write into the same buffer. When
 * it becomes clear that the result set does not fit in memory, the table switches to a mode where each thread
 * gets its own buffer and its own spill files on disk.
 */
public class ConcurrentGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private final List<SpillingGrouper<KeyType>> groupers;
  private final ThreadLocal<SpillingGrouper<KeyType>> threadLocalGrouper;
  private final AtomicInteger threadNumber = new AtomicInteger();
  private volatile boolean spilling = false;
  private volatile boolean closed = false;

  public ConcurrentGrouper(
      final ByteBuffer buffer,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float bufferGrouperMaxLoadFactor,
      final int bufferGrouperInitialBuckets,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int concurrencyHint
  )
  {
    Preconditions.checkArgument(concurrencyHint > 0, "concurrencyHint > 0");

    this.groupers = new ArrayList<>(concurrencyHint);
    this.threadLocalGrouper = new ThreadLocal<SpillingGrouper<KeyType>>()
    {
      @Override
      protected SpillingGrouper<KeyType> initialValue()
      {
        return groupers.get(threadNumber.getAndIncrement());
      }
    };

    final int sliceSize = (buffer.capacity() / concurrencyHint);

    for (int i = 0; i < concurrencyHint; i++) {
      final ByteBuffer slice = buffer.duplicate();
      slice.position(sliceSize * i);
      slice.limit(slice.position() + sliceSize);
      groupers.add(
          new SpillingGrouper<>(
              slice.slice(),
              keySerdeFactory,
              columnSelectorFactory,
              aggregatorFactories,
              bufferGrouperMaxSize,
              bufferGrouperMaxLoadFactor,
              bufferGrouperInitialBuckets,
              temporaryStorage,
              spillMapper,
              false
          )
      );
    }
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    if (closed) {
      throw new ISE("Grouper is closed");
    }

    if (!spilling) {
      final SpillingGrouper<KeyType> hashBasedGrouper = groupers.get(grouperNumberForKeyHash(keyHash));

      synchronized (hashBasedGrouper) {
        if (!spilling) {
          if (hashBasedGrouper.aggregate(key, keyHash)) {
            return true;
          } else {
            spilling = true;
          }
        }
      }
    }

    // At this point we know spilling = true
    final SpillingGrouper<KeyType> tlGrouper = threadLocalGrouper.get();

    synchronized (tlGrouper) {
      tlGrouper.setSpillingAllowed(true);
      return tlGrouper.aggregate(key, keyHash);
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    return aggregate(key, Groupers.hash(key));
  }

  @Override
  public void reset()
  {
    if (closed) {
      throw new ISE("Grouper is closed");
    }

    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.reset();
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (closed) {
      throw new ISE("Grouper is closed");
    }

    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(groupers.size());

    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        iterators.add(grouper.iterator(sorted));
      }
    }

    return Groupers.mergeIterators(iterators, sorted);
  }

  @Override
  public void close()
  {
    closed = true;
    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.close();
      }
    }
  }

  private int grouperNumberForKeyHash(int keyHash)
  {
    return keyHash % groupers.size();
  }
}
