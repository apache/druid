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
import com.google.common.primitives.Longs;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import org.junit.AfterClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentGrouperTest
{
  private static final ExecutorService service = Executors.newFixedThreadPool(8);

  @AfterClass
  public static void teardown()
  {
    service.shutdown();
  }

  private static final Supplier<ByteBuffer> bufferSupplier = new Supplier<ByteBuffer>()
  {
    private final AtomicBoolean called = new AtomicBoolean(false);

    @Override
    public ByteBuffer get()
    {
      if (called.compareAndSet(false, true)) {
        return ByteBuffer.allocate(192);
      } else {
        throw new IAE("should be called once");
      }
    }
  };

  private static final KeySerdeFactory<Long> keySerdeFactory = new KeySerdeFactory<Long>()
  {
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
  };

  private static final ColumnSelectorFactory null_factory = new ColumnSelectorFactory()
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return null;
    }
  };

  @Test
  public void testAggregate() throws InterruptedException, ExecutionException
  {
    final ConcurrentGrouper<Long> grouper = new ConcurrentGrouper<>(
        bufferSupplier,
        keySerdeFactory,
        null_factory,
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        24,
        0.7f,
        1,
        null,
        null,
        8,
        null,
        false
    );

    Future<?>[] futures = new Future[8];

    for (int i = 0; i < 8; i++) {
      futures[i] = service.submit(new Runnable()
      {
        @Override
        public void run()
        {
          grouper.init();
          for (long i = 0; i < 100; i++) {
            grouper.aggregate(0L);
          }
        }
      });
    }

    for (Future eachFuture : futures) {
      eachFuture.get();
    }

    grouper.close();
  }
}
