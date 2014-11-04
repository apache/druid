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

package io.druid.segment.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
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
  private RealtimeManager realtimeManager2;
  private DataSchema schema;
  private DataSchema schema2;
  private TestPlumber plumber;
  private TestPlumber plumber2;

  @Before
  public void setUp() throws Exception
  {
    final List<TestInputRowHolder> rows = Arrays.asList(
        makeRow(new DateTime("9000-01-01").getMillis()),
        makeRow(new ParseException("parse error")),
        null,
        makeRow(new DateTime().getMillis())
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.NONE, null),
        jsonMapper
    );
    schema2 = new DataSchema(
        "testV2",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.NONE, null),
        jsonMapper
    );
    RealtimeIOConfig ioConfig = new RealtimeIOConfig(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser) throws IOException
          {
            return new TestFirehose(rows.iterator());
          }
        },
        new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(
              DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
          )
          {
            return plumber;
          }
        },
        null
    );
    RealtimeIOConfig ioConfig2 = new RealtimeIOConfig(
        null,
        new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(
              DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
          )
          {
            return plumber2;
          }
        },
        new FirehoseFactoryV2()
        {
          @Override
          public FirehoseV2 connect(InputRowParser parser, Object arg1) throws IOException, ParseException
          {
            return new TestFirehoseV2(rows.iterator());
          }
        }
    );
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        1,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0
    );
    plumber = new TestPlumber(new Sink(new Interval("0/P5000Y"), schema, tuningConfig, new DateTime().toString()));

    realtimeManager = new RealtimeManager(
        Arrays.<FireDepartment>asList(
            new FireDepartment(
                schema,
                ioConfig,
                tuningConfig
            )
        ),
        null
    );
    plumber2 = new TestPlumber(new Sink(new Interval("0/P5000Y"), schema2, tuningConfig, new DateTime().toString()));

    realtimeManager2 = new RealtimeManager(
        Arrays.<FireDepartment>asList(
            new FireDepartment(
                schema2,
                ioConfig2,
                tuningConfig
            )
        ),
        null
    );
  }

  @Test
  public void testRun() throws Exception
  {
    realtimeManager.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    while (realtimeManager.getMetrics("test").processed() != 1) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Realtime manager should have completed processing 2 events!");
      }
    }

    Assert.assertEquals(1, realtimeManager.getMetrics("test").processed());
    Assert.assertEquals(1, realtimeManager.getMetrics("test").thrownAway());
    Assert.assertEquals(2, realtimeManager.getMetrics("test").unparseable());
    Assert.assertTrue(plumber.isStartedJob());
    Assert.assertTrue(plumber.isFinishedJob());
    Assert.assertEquals(0, plumber.getPersistCount());
  }

  @Test
  public void testRunV2() throws Exception
  {
    realtimeManager2.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    while (realtimeManager2.getMetrics("testV2").processed() != 1) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Realtime manager should have completed processing 2 events!");
      }
    }

    Assert.assertEquals(1, realtimeManager2.getMetrics("testV2").processed());
    Assert.assertEquals(1, realtimeManager2.getMetrics("testV2").thrownAway());
    Assert.assertEquals(2, realtimeManager2.getMetrics("testV2").unparseable());
    Assert.assertTrue(plumber2.isStartedJob());
    Assert.assertTrue(plumber2.isFinishedJob());
    Assert.assertEquals(0, plumber2.getPersistCount());
  }

  private TestInputRowHolder makeRow(final long timestamp)
  {
    return new TestInputRowHolder(timestamp, null);
  }

  private TestInputRowHolder makeRow(final RuntimeException e)
  {
    return new TestInputRowHolder(0, e);
  }

  private static class TestInputRowHolder
  {
    private long timestamp;
    private RuntimeException exception;

    public TestInputRowHolder(long timestamp, RuntimeException exception)
    {
      this.timestamp = timestamp;
      this.exception = exception;
    }

    public InputRow getRow()
    {
      if (exception != null) {
        throw exception;
      }

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
        public DateTime getTimestamp()
        {
          return new DateTime(timestamp);
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

        @Override
        public long getLongMetric(String metric)
        {
          return 0L;
        }

        @Override
        public Object getRaw(String dimension)
        {
          return null;
        }

        @Override
        public int compareTo(Row o)
        {
          return 0;
        }
      };
    }
  }

  private static class TestFirehose implements Firehose
  {
    private final Iterator<TestInputRowHolder> rows;

    private TestFirehose(Iterator<TestInputRowHolder> rows)
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
      final TestInputRowHolder holder = rows.next();
      if (holder == null) {
        return null;
      } else {
        return holder.getRow();
      }
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

  private static class TestFirehoseV2 implements FirehoseV2
  {
    private final Iterator<TestInputRowHolder> rows;
    private InputRow currRow;
    private boolean stop;

    private TestFirehoseV2(Iterator<TestInputRowHolder> rows)
    {
      this.rows = rows;
    }

    private void nextMessage()
    {
      currRow = null;
      while (currRow == null) {
        final TestInputRowHolder holder = rows.next();
        currRow = holder == null ? null : holder.getRow();
      }
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public boolean advance()
    {
      stop = !rows.hasNext();
      if (stop) {
        return false;
      }

      nextMessage();
      return true;
    }

    @Override
    public InputRow currRow()
    {
      return currRow;
    }

    @Override
    public Committer makeCommitter()
    {
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return null;
        }

        @Override
        public void run()
        {
        }
      };
    }

    @Override
    public void start() throws Exception
    {
      nextMessage();
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
    public Object startJob()
    {
      startedJob = true;
      return null;
    }

    @Override
    public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
    {
      if (row == null) {
        return -1;
      }

      Sink sink = getSink(row.getTimestampFromEpoch());

      if (sink == null) {
        return -1;
      }

      return sink.add(row);
    }

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
    public void persist(Committer committer)
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
