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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FiniteAppenderatorDriverTest
{
  private static final String DATA_SOURCE = "foo";
  private static final String VERSION = "abc123";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final int MAX_ROWS_IN_MEMORY = 100;
  private static final int MAX_ROWS_PER_SEGMENT = 3;
  private static final long HANDOFF_CONDITION_TIMEOUT = 0;

  private static final List<InputRow> ROWS = Arrays.<InputRow>asList(
      new MapBasedInputRow(
          new DateTime("2000"),
          ImmutableList.of("dim1"),
          ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
      ),
      new MapBasedInputRow(
          new DateTime("2000T01"),
          ImmutableList.of("dim1"),
          ImmutableMap.<String, Object>of("dim1", "foo", "met1", 2.0)
      ),
      new MapBasedInputRow(
          new DateTime("2000T01"),
          ImmutableList.of("dim2"),
          ImmutableMap.<String, Object>of("dim2", "bar", "met1", 2.0)
      )
  );

  SegmentAllocator allocator;
  AppenderatorTester appenderatorTester;
  FiniteAppenderatorDriver driver;

  @Before
  public void setUp()
  {
    appenderatorTester = new AppenderatorTester(MAX_ROWS_IN_MEMORY);
    allocator = new TestSegmentAllocator(DATA_SOURCE, Granularity.HOUR);
    driver = new FiniteAppenderatorDriver(
        appenderatorTester.getAppenderator(),
        allocator,
        new TestSegmentHandoffNotifierFactory(),
        new TestUsedSegmentChecker(),
        OBJECT_MAPPER,
        MAX_ROWS_PER_SEGMENT,
        HANDOFF_CONDITION_TIMEOUT
    );
  }

  @After
  public void tearDown() throws Exception
  {
    driver.clear();
    driver.close();
  }

  @Test
  public void testSimple() throws Exception
  {
    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();

    Assert.assertNull(driver.startJob());

    for (int i = 0; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertNotNull(driver.add(ROWS.get(i), "dummy", committerSupplier));
    }

    final SegmentsAndMetadata segmentsAndMetadata = driver.finish(
        makeOkPublisher(),
        committerSupplier.get()
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdentifier(DATA_SOURCE, new Interval("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0)),
            new SegmentIdentifier(DATA_SOURCE, new Interval("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0))
        ),
        asIdentifiers(segmentsAndMetadata.getSegments())
    );

    Assert.assertEquals(3, segmentsAndMetadata.getCommitMetadata());
  }

  @Test
  public void testMaxRowsPerSegment() throws Exception
  {
    final int numSegments = 3;
    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();
    Assert.assertNull(driver.startJob());

    for (int i = 0; i < numSegments * MAX_ROWS_PER_SEGMENT; i++) {
      committerSupplier.setMetadata(i + 1);
      InputRow row = new MapBasedInputRow(
          new DateTime("2000T01"),
          ImmutableList.of("dim2"),
          ImmutableMap.<String, Object>of(
              "dim2",
              String.format("bar-%d", i),
              "met1",
              2.0
          )
      );
      Assert.assertNotNull(driver.add(row, "dummy", committerSupplier));
    }

    final SegmentsAndMetadata segmentsAndMetadata = driver.finish(makeOkPublisher(), committerSupplier.get());
    Assert.assertEquals(numSegments, segmentsAndMetadata.getSegments().size());
    Assert.assertEquals(numSegments * MAX_ROWS_PER_SEGMENT, segmentsAndMetadata.getCommitMetadata());
  }

  private Set<SegmentIdentifier> asIdentifiers(Iterable<DataSegment> segments)
  {
    return ImmutableSet.copyOf(
        Iterables.transform(
            segments,
            new Function<DataSegment, SegmentIdentifier>()
            {
              @Override
              public SegmentIdentifier apply(DataSegment input)
              {
                return SegmentIdentifier.fromDataSegment(input);
              }
            }
        )
    );
  }

  private TransactionalSegmentPublisher makeOkPublisher()
  {
    return new TransactionalSegmentPublisher()
    {
      @Override
      public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
      {
        return true;
      }
    };
  }

  private class TestCommitterSupplier<T> implements Supplier<Committer>
  {
    private final AtomicReference<T> metadata = new AtomicReference<>();

    public void setMetadata(T newMetadata)
    {
      metadata.set(newMetadata);
    }

    @Override
    public Committer get()
    {
      final T currentMetadata = metadata.get();
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return currentMetadata;
        }

        @Override
        public void run()
        {
          // Do nothing
        }
      };
    }
  }

  private class TestSegmentAllocator implements SegmentAllocator
  {
    private final String dataSource;
    private final Granularity granularity;
    private final Map<Long, AtomicInteger> counters = Maps.newHashMap();

    public TestSegmentAllocator(String dataSource, Granularity granularity)
    {
      this.dataSource = dataSource;
      this.granularity = granularity;
    }

    @Override
    public SegmentIdentifier allocate(
        final DateTime timestamp,
        final String sequenceName,
        final String previousSegmentId
    ) throws IOException
    {
      synchronized (counters) {
        final long timestampTruncated = granularity.truncate(timestamp).getMillis();
        if (!counters.containsKey(timestampTruncated)) {
          counters.put(timestampTruncated, new AtomicInteger());
        }
        final int partitionNum = counters.get(timestampTruncated).getAndIncrement();
        return new SegmentIdentifier(
            dataSource,
            granularity.bucket(new DateTime(timestampTruncated)),
            VERSION,
            new NumberedShardSpec(partitionNum, 0)
        );
      }
    }
  }

  private class TestSegmentHandoffNotifierFactory implements SegmentHandoffNotifierFactory
  {
    @Override
    public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
    {
      return new SegmentHandoffNotifier()
      {
        @Override
        public boolean registerSegmentHandoffCallback(
            final SegmentDescriptor descriptor,
            final Executor exec,
            final Runnable handOffRunnable
        )
        {
          // Immediate handoff
          exec.execute(handOffRunnable);
          return true;
        }

        @Override
        public void start()
        {
          // Do nothing
        }

        @Override
        public void close()
        {
          // Do nothing
        }
      };
    }
  }

  private class TestUsedSegmentChecker implements UsedSegmentChecker
  {
    @Override
    public Set<DataSegment> findUsedSegments(Set<SegmentIdentifier> identifiers) throws IOException
    {
      final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
      for (DataSegment dataSegment : appenderatorTester.getPushedSegments()) {
        timeline.add(
            dataSegment.getInterval(),
            dataSegment.getVersion(),
            dataSegment.getShardSpec().createChunk(dataSegment)
        );
      }

      final Set<DataSegment> retVal = Sets.newHashSet();
      for (SegmentIdentifier identifier : identifiers) {
        for (TimelineObjectHolder<String, DataSegment> holder : timeline.lookup(identifier.getInterval())) {
          for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
            if (identifiers.contains(SegmentIdentifier.fromDataSegment(chunk.getObject()))) {
              retVal.add(chunk.getObject());
            }
          }
        }
      }

      return retVal;
    }
  }
}
