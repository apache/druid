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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.collections.ReferenceCountingResourceHolder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import io.druid.query.groupby.epinephelinae.Grouper.Entry;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ColumnCapabilities;
import org.junit.AfterClass;
import org.junit.Assert;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class ConcurrentGrouperTest
{
  private static final ExecutorService SERVICE = Executors.newFixedThreadPool(8);
  private static final TestResourceHolder TEST_RESOURCE_HOLDER = new TestResourceHolder(256);
  private static final KeySerdeFactory<Long> KEY_SERDE_FACTORY = new TestKeySerdeFactory();
  private static final ColumnSelectorFactory NULL_FACTORY = new TestColumnSelectorFactory();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Supplier<ByteBuffer> bufferSupplier;

  @Parameters(name = "bufferSize={0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{1024 * 32},
        new Object[]{1024 * 1024}
    );
  }

  @AfterClass
  public static void teardown()
  {
    SERVICE.shutdown();
  }

  public ConcurrentGrouperTest(int bufferSize)
  {
    bufferSupplier = new Supplier<ByteBuffer>()
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
  }

  @Test()
  public void testAggregate() throws InterruptedException, ExecutionException, IOException
  {
    final ConcurrentGrouper<Long> grouper = new ConcurrentGrouper<>(
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
        8,
        null,
        false,
        MoreExecutors.listeningDecorator(SERVICE),
        0,
        false,
        0,
        4,
        8
    );
    grouper.init();

    final int numRows = 1000;

    Future<?>[] futures = new Future[8];

    for (int i = 0; i < 8; i++) {
      futures[i] = SERVICE.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (long i = 0; i < numRows; i++) {
            grouper.aggregate(i);
          }
        }
      });
    }

    for (Future eachFuture : futures) {
      eachFuture.get();
    }

    final CloseableIterator<Entry<Long>> iterator = grouper.iterator(true);
    final List<Entry<Long>> actual = Lists.newArrayList(iterator);
    iterator.close();

    Assert.assertTrue(TEST_RESOURCE_HOLDER.taken);

    final List<Entry<Long>> expected = new ArrayList<>();
    for (long i = 0; i < numRows; i++) {
      expected.add(new Entry<>(i, new Object[]{8L}));
    }

    Assert.assertEquals(expected, actual);

    grouper.close();
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

  static class TestKeySerdeFactory implements KeySerdeFactory<Long>
  {
    @Override
    public long getMaxDictionarySize()
    {
      return 0;
    }

    @Override
    public KeySerde<Long> factorize()
    {
      return new KeySerde<Long>()
      {
        final ByteBuffer buffer = ByteBuffer.allocate(8);

        @Override
        public int keySize()
        {
          return 8;
        }

        @Override
        public Class<Long> keyClazz()
        {
          return Long.class;
        }

        @Override
        public List<String> getDictionary()
        {
          return ImmutableList.of();
        }

        @Override
        public ByteBuffer toByteBuffer(Long key)
        {
          buffer.rewind();
          buffer.putLong(key);
          buffer.position(0);
          return buffer;
        }

        @Override
        public Long fromByteBuffer(ByteBuffer buffer, int position)
        {
          return buffer.getLong(position);
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
        public void reset() {}
      };
    }

    @Override
    public KeySerde<Long> factorizeWithDictionary(List<String> dictionary)
    {
      return factorize();
    }

    @Override
    public Comparator<Grouper.Entry<Long>> objectComparator(boolean forceDefaultOrder)
    {
      return new Comparator<Grouper.Entry<Long>>()
      {
        @Override
        public int compare(Grouper.Entry<Long> o1, Grouper.Entry<Long> o2)
        {
          return o1.getKey().compareTo(o2.getKey());
        }
      };
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
