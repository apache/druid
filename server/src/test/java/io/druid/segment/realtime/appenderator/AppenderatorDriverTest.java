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
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.realtime.FireDepartmentMetrics;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AppenderatorDriverTest
{
  private static final String DATA_SOURCE = "foo";
  private static final String VERSION = "abc123";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final int MAX_ROWS_IN_MEMORY = 100;
  private static final int MAX_ROWS_PER_SEGMENT = 3;
  private static final long PUBLISH_TIMEOUT = 10000;
  private static final long HANDOFF_CONDITION_TIMEOUT = 1000;

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
  TestSegmentHandoffNotifierFactory segmentHandoffNotifierFactory;
  AppenderatorDriver driver;

  @Before
  public void setUp()
  {
    appenderatorTester = new AppenderatorTester(MAX_ROWS_IN_MEMORY);
    allocator = new TestSegmentAllocator(DATA_SOURCE, Granularities.HOUR);
    segmentHandoffNotifierFactory = new TestSegmentHandoffNotifierFactory();
    driver = new AppenderatorDriver(
        appenderatorTester.getAppenderator(),
        allocator,
        segmentHandoffNotifierFactory,
        new TestUsedSegmentChecker(),
        OBJECT_MAPPER,
        new FireDepartmentMetrics()
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
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());
    }

    final SegmentsAndMetadata published = driver.publish(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertFalse(driver.getActiveSegments().containsKey("dummy"));
    Assert.assertFalse(driver.getPublishPendingSegments().containsKey("dummy"));
    final SegmentsAndMetadata segmentsAndMetadata = driver.registerHandoff(published)
                                                          .get(HANDOFF_CONDITION_TIMEOUT, TimeUnit.MILLISECONDS);

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
          ImmutableMap.of(
              "dim2",
              StringUtils.format("bar-%d", i),
              "met1",
              2.0
          )
      );
      final AppenderatorDriverAddResult addResult = driver.add(row, "dummy", committerSupplier);
      Assert.assertTrue(addResult.isOk());
      if (addResult.getNumRowsInSegment() > MAX_ROWS_PER_SEGMENT) {
        driver.moveSegmentOut("dummy", ImmutableList.of(addResult.getSegmentIdentifier()));
      }
    }

    final SegmentsAndMetadata published = driver.publish(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertFalse(driver.getActiveSegments().containsKey("dummy"));
    Assert.assertFalse(driver.getPublishPendingSegments().containsKey("dummy"));
    final SegmentsAndMetadata segmentsAndMetadata = driver.registerHandoff(published)
                                                          .get(HANDOFF_CONDITION_TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertEquals(numSegments, segmentsAndMetadata.getSegments().size());
    Assert.assertEquals(numSegments * MAX_ROWS_PER_SEGMENT, segmentsAndMetadata.getCommitMetadata());
  }

  @Test(timeout = 5000L, expected = TimeoutException.class)
  public void testHandoffTimeout() throws Exception
  {
    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();
    segmentHandoffNotifierFactory.disableHandoff();

    Assert.assertNull(driver.startJob());

    for (int i = 0; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());
    }

    final SegmentsAndMetadata published = driver.publish(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertFalse(driver.getActiveSegments().containsKey("dummy"));
    Assert.assertFalse(driver.getPublishPendingSegments().containsKey("dummy"));
    driver.registerHandoff(published).get(HANDOFF_CONDITION_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testPublishPerRow() throws IOException, InterruptedException, TimeoutException, ExecutionException
  {
    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();

    Assert.assertNull(driver.startJob());

    // Add the first row and publish immediately
    {
      committerSupplier.setMetadata(1);
      Assert.assertTrue(driver.add(ROWS.get(0), "dummy", committerSupplier).isOk());

      final SegmentsAndMetadata segmentsAndMetadata = driver.publishAndRegisterHandoff(
          makeOkPublisher(),
          committerSupplier.get(),
          ImmutableList.of("dummy")
      ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);

      Assert.assertEquals(
          ImmutableSet.of(
              new SegmentIdentifier(DATA_SOURCE, new Interval("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0))
          ),
          asIdentifiers(segmentsAndMetadata.getSegments())
      );

      Assert.assertEquals(1, segmentsAndMetadata.getCommitMetadata());
    }

    // Add the second and third rows and publish immediately
    for (int i = 1; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());

      final SegmentsAndMetadata segmentsAndMetadata = driver.publishAndRegisterHandoff(
          makeOkPublisher(),
          committerSupplier.get(),
          ImmutableList.of("dummy")
      ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);

      Assert.assertEquals(
          ImmutableSet.of(
              // The second and third rows have the same dataSource, interval, and version, but different shardSpec of
              // different partitionNum
              new SegmentIdentifier(DATA_SOURCE, new Interval("2000T01/PT1H"), VERSION, new NumberedShardSpec(i - 1, 0))
          ),
          asIdentifiers(segmentsAndMetadata.getSegments())
      );

      Assert.assertEquals(i + 1, segmentsAndMetadata.getCommitMetadata());
    }

    driver.persist(committerSupplier.get());

    // There is no remaining rows in the driver, and thus the result must be empty
    final SegmentsAndMetadata segmentsAndMetadata = driver.publishAndRegisterHandoff(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);

    Assert.assertEquals(
        ImmutableSet.of(),
        asIdentifiers(segmentsAndMetadata.getSegments())
    );

    Assert.assertEquals(3, segmentsAndMetadata.getCommitMetadata());
  }

  @Test
  public void testIncrementalHandoff() throws Exception
  {
    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();

    Assert.assertNull(driver.startJob());

    committerSupplier.setMetadata(1);
    Assert.assertTrue(driver.add(ROWS.get(0), "sequence_0", committerSupplier).isOk());

    for (int i = 1; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "sequence_1", committerSupplier).isOk());
    }

    final ListenableFuture<SegmentsAndMetadata> futureForSequence0 = driver.publishAndRegisterHandoff(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("sequence_0")
    );

    final ListenableFuture<SegmentsAndMetadata> futureForSequence1 = driver.publishAndRegisterHandoff(
        makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("sequence_1")
    );

    final SegmentsAndMetadata handedoffFromSequence0 = futureForSequence0.get(
        HANDOFF_CONDITION_TIMEOUT,
        TimeUnit.MILLISECONDS
    );
    final SegmentsAndMetadata handedoffFromSequence1 = futureForSequence1.get(
        HANDOFF_CONDITION_TIMEOUT,
        TimeUnit.MILLISECONDS
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdentifier(DATA_SOURCE, new Interval("2000/PT1H"), VERSION, new NumberedShardSpec(0, 0))
        ),
        asIdentifiers(handedoffFromSequence0.getSegments())
    );

    Assert.assertEquals(
        ImmutableSet.of(
            new SegmentIdentifier(DATA_SOURCE, new Interval("2000T01/PT1H"), VERSION, new NumberedShardSpec(0, 0))
        ),
        asIdentifiers(handedoffFromSequence1.getSegments())
    );

    Assert.assertEquals(3, handedoffFromSequence0.getCommitMetadata());
    Assert.assertEquals(3, handedoffFromSequence1.getCommitMetadata());
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

  static TransactionalSegmentPublisher makeOkPublisher()
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

  static class TestCommitterSupplier<T> implements Supplier<Committer>
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

  static class TestSegmentAllocator implements SegmentAllocator
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
        final InputRow row,
        final String sequenceName,
        final String previousSegmentId
    ) throws IOException
    {
      synchronized (counters) {
        final long timestampTruncated = granularity.bucketStart(row.getTimestamp()).getMillis();
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

  static class TestSegmentHandoffNotifierFactory implements SegmentHandoffNotifierFactory
  {
    private boolean handoffEnabled = true;
    private long handoffDelay;

    public void disableHandoff()
    {
      handoffEnabled = false;
    }

    public void setHandoffDelay(long delay)
    {
      handoffDelay = delay;
    }

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
          if (handoffEnabled) {

            if (handoffDelay > 0) {
              try {
                Thread.sleep(handoffDelay);
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            exec.execute(handOffRunnable);
          }
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
