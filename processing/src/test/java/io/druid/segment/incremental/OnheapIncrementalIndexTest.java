package io.druid.segment.incremental;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class OnheapIncrementalIndexTest
{

  private static final int MAX_ROWS = 100000;

  @Test
  public void testMultithreadAddFacts() throws Exception
  {
    final OnheapIncrementalIndex index = new OnheapIncrementalIndex(
        0,
        QueryGranularities.MINUTE,
        new AggregatorFactory[]{new LongMaxAggregatorFactory("max", "max")},
        MAX_ROWS
    );

    final Random random = new Random();
    final int addThreadCount = 2;
    Thread[] addThreads = new Thread[addThreadCount];
    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i] = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            for (int j = 0; j < MAX_ROWS / addThreadCount; ++j) {
              index.add(new MapBasedInputRow(
                  0,
                  Lists.newArrayList("billy"),
                  ImmutableMap.<String, Object>of("billy", random.nextLong(), "max", 1)
              ));
            }
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
      addThreads[i].start();
    }

    final AtomicInteger checkFailedCount = new AtomicInteger(0);
    Thread checkThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        while (!Thread.interrupted()) {
          for (int row : index.getFacts().values()) {
            if (index.getMetricLongValue(row, 0) != 1) {
              checkFailedCount.addAndGet(1);
            }
          }
        }
      }
    });
    checkThread.start();

    for (int i = 0; i < addThreadCount; ++i) {
      addThreads[i].join();
    }
    checkThread.interrupt();

    Assert.assertEquals(0, checkFailedCount.get());
  }
}