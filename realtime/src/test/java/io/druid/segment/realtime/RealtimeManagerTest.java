/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.common.guava.Runnables;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.incremental.SpatialDimensionSchema;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.timeline.partition.NoneShardSpec;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimeManagerTest
{
  private RealtimeManager realtimeManager;
  private Schema schema;
  private TestPlumber plumber;

  @Before
  public void setUp() throws Exception
  {
    schema = new Schema(
        "test",
        Lists.<SpatialDimensionSchema>newArrayList(),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        QueryGranularity.NONE,
        new NoneShardSpec()
    );

    final List<InputRow> rows = Arrays.asList(
        makeRow(new DateTime("9000-01-01").getMillis()), makeRow(new DateTime().getMillis())
    );

    plumber = new TestPlumber(new Sink(new Interval("0/P5000Y"), schema, new DateTime().toString()));

    realtimeManager = new RealtimeManager(
        Arrays.<FireDepartment>asList(
            new FireDepartment(
                schema,
                new FireDepartmentConfig(1, new Period("P1Y")),
                new FirehoseFactory()
                {
                  @Override
                  public Firehose connect() throws IOException
                  {
                    return new TestFirehose(rows.iterator());
                  }
                },
                new PlumberSchool()
                {
                  @Override
                  public Plumber findPlumber(
                      Schema schema, FireDepartmentMetrics metrics
                  )
                  {
                    return plumber;
                  }
                }
            )
        ),
        null
    );
  }

  @Test
  public void testRun() throws Exception
  {
    realtimeManager.start();

    Stopwatch stopwatch = new Stopwatch().start();
    while (realtimeManager.getMetrics("test").processed() != 1) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Realtime manager should have completed processing 2 events!");
      }
    }

    Assert.assertEquals(1, realtimeManager.getMetrics("test").processed());
    Assert.assertEquals(1, realtimeManager.getMetrics("test").thrownAway());
    Assert.assertTrue(plumber.isStartedJob());
    Assert.assertTrue(plumber.isFinishedJob());
    Assert.assertEquals(1, plumber.getPersistCount());
  }

  private InputRow makeRow(final long timestamp)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return Arrays.asList("testDim");
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return timestamp;
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return Lists.newArrayList();
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return 0;
      }
    };
  }


  private static class TestFirehose implements Firehose
  {
    private final Iterator<InputRow> rows;

    private TestFirehose(Iterator<InputRow> rows)
    {
      this.rows = rows;
    }

    @Override
    public boolean hasMore()
    {
      return rows.hasNext();
    }

    @Override
    public InputRow nextRow()
    {
      return rows.next();
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    @Override
    public void close() throws IOException
    {
    }
  }

  private static class TestPlumber implements Plumber
  {
    private final Sink sink;


    private volatile boolean startedJob = false;
    private volatile boolean finishedJob = false;
    private volatile int persistCount = 0;

    private TestPlumber(Sink sink)
    {
      this.sink = sink;
    }

    private boolean isStartedJob()
    {
      return startedJob;
    }

    private boolean isFinishedJob()
    {
      return finishedJob;
    }

    private int getPersistCount()
    {
      return persistCount;
    }

    @Override
    public void startJob()
    {
      startedJob = true;
    }

    @Override
    public Sink getSink(long timestamp)
    {
      if (sink.getInterval().contains(timestamp)) {
        return sink;
      }
      return null;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void persist(Runnable commitRunnable)
    {
      persistCount++;
    }

    @Override
    public void finishJob()
    {
      finishedJob = true;
    }
  }
}
