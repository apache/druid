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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class ConcurrentGrouperTest extends InitializedNullHandlingTest
{
  private static final TestResourceHolder TEST_RESOURCE_HOLDER = new TestResourceHolder(256);
  private static final KeySerdeFactory<LongKey> KEY_SERDE_FACTORY = new TestKeySerdeFactory();
  private static final ColumnSelectorFactory NULL_FACTORY = new TestColumnSelectorFactory();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Supplier<ByteBuffer> bufferSupplier;
  private final int concurrencyHint;
  private final int parallelCombineThreads;
  private final ExecutorService exec;
  private final boolean mergeThreadLocal;
  private final Closer closer = Closer.create();

  @Parameters(name = "bufferSize={0}, concurrencyHint={1}, parallelCombineThreads={2}, mergeThreadLocal={3}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (final int bufferSize : new int[]{1024, 1024 * 32, 1024 * 1024}) {
      for (final int concurrencyHint : new int[]{1, 8}) {
        for (final int parallelCombineThreads : new int[]{0, 8}) {
          for (final boolean mergeThreadLocal : new boolean[]{true, false}) {
            if (parallelCombineThreads <= concurrencyHint) {
              constructors.add(new Object[]{bufferSize, concurrencyHint, parallelCombineThreads, mergeThreadLocal});
            }
          }
        }
      }
    }

    return constructors;
  }

  @Before
  public void setUp()
  {
    TEST_RESOURCE_HOLDER.taken = false;
  }

  @After
  public void tearDown() throws IOException
  {
    exec.shutdownNow();
    closer.close();
  }

  public ConcurrentGrouperTest(
      int bufferSize,
      int concurrencyHint,
      int parallelCombineThreads,
      boolean mergeThreadLocal
  )
  {
    this.concurrencyHint = concurrencyHint;
    this.parallelCombineThreads = parallelCombineThreads;
    this.mergeThreadLocal = mergeThreadLocal;
    this.bufferSupplier = new Supplier<ByteBuffer>()
    {
      private final AtomicBoolean called = new AtomicBoolean(false);
      private ByteBuffer buffer;

      @Override
      public ByteBuffer get()
      {
        if (called.compareAndSet(false, true)) {
          buffer = ByteBuffer.allocate(bufferSize);
        }

        return buffer;
      }
    };
    this.exec = Execs.multiThreaded(concurrencyHint, "ConcurrentGrouperTest-%d");
  }

  @Test()
  public void testAggregate() throws InterruptedException, ExecutionException, IOException
  {
    final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
        temporaryFolder.newFolder(),
        1024 * 1024
    );

    final ConcurrentGrouper<LongKey> grouper = new ConcurrentGrouper<>(
        bufferSupplier,
        TEST_RESOURCE_HOLDER,
        KEY_SERDE_FACTORY,
        KEY_SERDE_FACTORY,
        NULL_FACTORY,
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        1024,
        0.7f,
        1,
        temporaryStorage,
        new DefaultObjectMapper(),
        concurrencyHint,
        null,
        false,
        MoreExecutors.listeningDecorator(exec),
        0,
        false,
        0,
        4,
        parallelCombineThreads,
        mergeThreadLocal
    );
    closer.register(grouper);
    grouper.init();

    final int numRows = 1000;

    Future<?>[] futures = new Future[concurrencyHint];

    for (int i = 0; i < concurrencyHint; i++) {
      futures[i] = exec.submit(() -> {
        for (long j = 0; j < numRows; j++) {
          if (!grouper.aggregate(new LongKey(j)).isOk()) {
            throw new ISE("Grouper is full");
          }
        }
      });
    }

    for (Future eachFuture : futures) {
      eachFuture.get();
    }

    final List<Entry<LongKey>> expected = new ArrayList<>();
    for (long i = 0; i < numRows; i++) {
      expected.add(new ReusableEntry<>(new LongKey(i), new Object[]{(long) concurrencyHint}));
    }

    final CloseableIterator<Entry<LongKey>> iterator = closer.register(grouper.iterator(true));

    if (parallelCombineThreads > 1 && (mergeThreadLocal || temporaryStorage.currentSize() > 0)) {
      // Parallel combiner configured, and expected to actually be used due to thread-local merge (either explicitly
      // configured, or due to spilling).
      Assert.assertTrue(TEST_RESOURCE_HOLDER.taken);
    } else {
      Assert.assertFalse(TEST_RESOURCE_HOLDER.taken);
    }

    GrouperTestUtil.assertEntriesEquals(expected.iterator(), iterator);
  }

  @Test
  public void testGrouperTimeout() throws Exception
  {
    if (concurrencyHint <= 1) {
      // Can't parallel sort. Timeout is only applied during parallel sorting, so this test is not useful. Skip it.
      return;
    }

    final ConcurrentGrouper<LongKey> grouper = new ConcurrentGrouper<>(
        bufferSupplier,
        TEST_RESOURCE_HOLDER,
        KEY_SERDE_FACTORY,
        KEY_SERDE_FACTORY,
        NULL_FACTORY,
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        1024,
        0.7f,
        1,
        new LimitedTemporaryStorage(temporaryFolder.newFolder(), 1024 * 1024),
        new DefaultObjectMapper(),
        concurrencyHint,
        null,
        false,
        MoreExecutors.listeningDecorator(exec),
        0,
        true,
        1,
        4,
        parallelCombineThreads,
        mergeThreadLocal
    );
    closer.register(grouper);
    grouper.init();

    final int numRows = 1000;

    Future<?>[] futures = new Future[concurrencyHint];

    for (int i = 0; i < concurrencyHint; i++) {
      futures[i] = exec.submit(() -> {
        for (long j = 0; j < numRows; j++) {
          if (!grouper.aggregate(new LongKey(j)).isOk()) {
            throw new ISE("Grouper is full");
          }
        }
      });
    }

    for (Future eachFuture : futures) {
      eachFuture.get();
    }

    final QueryTimeoutException e = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> closer.register(grouper.iterator(true))
    );

    Assert.assertEquals("Query timeout", e.getErrorCode());
  }

  static class TestResourceHolder extends ReferenceCountingResourceHolder<ByteBuffer>
  {
    private boolean taken;

    TestResourceHolder(int bufferSize)
    {
      super(ByteBuffer.allocate(bufferSize), () -> {});
    }

    @Override
    public ByteBuffer get()
    {
      taken = true;
      return super.get();
    }
  }

  static class LongKey
  {
    private long longValue;

    @JsonCreator
    public LongKey(final long longValue)
    {
      this.longValue = longValue;
    }

    @JsonValue
    public long longValue()
    {
      return longValue;
    }

    public void setValue(final long longValue)
    {
      this.longValue = longValue;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LongKey longKey = (LongKey) o;
      return longValue == longKey.longValue;
    }

    @Override
    public int hashCode()
    {
      return Long.hashCode(longValue);
    }

    @Override
    public String toString()
    {
      return "LongKey{" +
             "longValue=" + longValue +
             '}';
    }
  }

  static class TestKeySerdeFactory implements KeySerdeFactory<LongKey>
  {
    @Override
    public long getMaxDictionarySize()
    {
      return 0;
    }

    @Override
    public KeySerde<LongKey> factorize()
    {
      return new KeySerde<LongKey>()
      {
        final ByteBuffer buffer = ByteBuffer.allocate(8);

        @Override
        public int keySize()
        {
          return 8;
        }

        @Override
        public Class<LongKey> keyClazz()
        {
          return LongKey.class;
        }

        @Override
        public List<String> getDictionary()
        {
          return ImmutableList.of();
        }

        @Override
        public ByteBuffer toByteBuffer(LongKey key)
        {
          buffer.rewind();
          buffer.putLong(key.longValue());
          buffer.position(0);
          return buffer;
        }

        @Override
        public LongKey createKey()
        {
          return new LongKey(0);
        }

        @Override
        public void readFromByteBuffer(LongKey key, ByteBuffer buffer, int position)
        {
          key.setValue(buffer.getLong(position));
        }

        @Override
        public BufferComparator bufferComparator()
        {
          return new BufferComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
            }
          };
        }

        @Override
        public BufferComparator bufferComparatorWithAggregators(
            AggregatorFactory[] aggregatorFactories,
            int[] aggregatorOffsets
        )
        {
          return null;
        }

        @Override
        public void reset()
        {
        }
      };
    }

    @Override
    public KeySerde<LongKey> factorizeWithDictionary(List<String> dictionary)
    {
      return factorize();
    }

    @Override
    public LongKey copyKey(LongKey key)
    {
      return new LongKey(key.longValue());
    }

    @Override
    public Comparator<Grouper.Entry<LongKey>> objectComparator(boolean forceDefaultOrder)
    {
      return Comparator.comparingLong(o -> o.getKey().longValue());
    }
  }

  private static class TestColumnSelectorFactory implements ColumnSelectorFactory
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  }
}
