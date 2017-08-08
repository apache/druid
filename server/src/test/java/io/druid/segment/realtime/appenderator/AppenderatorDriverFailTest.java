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
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.AppenderatorDriverTest.TestCommitterSupplier;
import io.druid.segment.realtime.appenderator.AppenderatorDriverTest.TestSegmentAllocator;
import io.druid.segment.realtime.appenderator.AppenderatorDriverTest.TestSegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class AppenderatorDriverFailTest
{
  private static final String DATA_SOURCE = "foo";
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final long PUBLISH_TIMEOUT = 5000;

  private static final List<InputRow> ROWS = ImmutableList.of(
      new MapBasedInputRow(
          new DateTime("2000"),
          ImmutableList.of("dim1"),
          ImmutableMap.of("dim1", "foo", "met1", "1")
      ),
      new MapBasedInputRow(
          new DateTime("2000T01"),
          ImmutableList.of("dim1"),
          ImmutableMap.of("dim1", "foo", "met1", 2.0)
      ),
      new MapBasedInputRow(
          new DateTime("2000T01"),
          ImmutableList.of("dim2"),
          ImmutableMap.of("dim2", "bar", "met1", 2.0)
      )
  );

  SegmentAllocator allocator;
  TestSegmentHandoffNotifierFactory segmentHandoffNotifierFactory;
  AppenderatorDriver driver;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    allocator = new TestSegmentAllocator(DATA_SOURCE, Granularities.HOUR);
    segmentHandoffNotifierFactory = new TestSegmentHandoffNotifierFactory();
  }

  @After
  public void tearDown() throws Exception
  {
    if (driver != null) {
      driver.clear();
      driver.close();
    }
  }

  @Test
  public void testFailDuringPersist() throws IOException, InterruptedException, TimeoutException, ExecutionException
  {
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ISE.class));
    expectedException.expectMessage("Fail test while persisting segments"
                                    + "[[foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123, "
                                    + "foo_2000-01-01T01:00:00.000Z_2000-01-01T02:00:00.000Z_abc123]]");

    driver = new AppenderatorDriver(
        createPersistFailAppenderator(),
        allocator,
        segmentHandoffNotifierFactory,
        new NoopUsedSegmentChecker(),
        OBJECT_MAPPER,
        new FireDepartmentMetrics()
    );

    driver.startJob();

    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();
    segmentHandoffNotifierFactory.setHandoffDelay(100);

    Assert.assertNull(driver.startJob());

    for (int i = 0; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());
    }

    driver.publish(
        AppenderatorDriverTest.makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testFailDuringPush() throws IOException, InterruptedException, TimeoutException, ExecutionException
  {
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ISE.class));
    expectedException.expectMessage("Fail test while pushing segments"
                                    + "[[foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123, "
                                    + "foo_2000-01-01T01:00:00.000Z_2000-01-01T02:00:00.000Z_abc123]]");

    driver = new AppenderatorDriver(
        createPushFailAppenderator(),
        allocator,
        segmentHandoffNotifierFactory,
        new NoopUsedSegmentChecker(),
        OBJECT_MAPPER,
        new FireDepartmentMetrics()
    );

    driver.startJob();

    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();
    segmentHandoffNotifierFactory.setHandoffDelay(100);

    Assert.assertNull(driver.startJob());

    for (int i = 0; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());
    }

    driver.publish(
        AppenderatorDriverTest.makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testFailDuringDrop() throws IOException, InterruptedException, TimeoutException, ExecutionException
  {
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ISE.class));
    expectedException.expectMessage(
        "Fail test while dropping segment[foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123]"
    );

    driver = new AppenderatorDriver(
        createDropFailAppenderator(),
        allocator,
        segmentHandoffNotifierFactory,
        new NoopUsedSegmentChecker(),
        OBJECT_MAPPER,
        new FireDepartmentMetrics()
    );

    driver.startJob();

    final TestCommitterSupplier<Integer> committerSupplier = new TestCommitterSupplier<>();
    segmentHandoffNotifierFactory.setHandoffDelay(100);

    Assert.assertNull(driver.startJob());

    for (int i = 0; i < ROWS.size(); i++) {
      committerSupplier.setMetadata(i + 1);
      Assert.assertTrue(driver.add(ROWS.get(i), "dummy", committerSupplier).isOk());
    }

    final SegmentsAndMetadata published = driver.publish(
        AppenderatorDriverTest.makeOkPublisher(),
        committerSupplier.get(),
        ImmutableList.of("dummy")
    ).get(PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS);

    driver.registerHandoff(published).get();
  }

  private static class NoopUsedSegmentChecker implements UsedSegmentChecker
  {
    @Override
    public Set<DataSegment> findUsedSegments(Set<SegmentIdentifier> identifiers) throws IOException
    {
      return ImmutableSet.of();
    }
  }

  static Appenderator createPushFailAppenderator()
  {
    return new FailableAppenderator().disablePush();
  }

  static Appenderator createPushInterruptAppenderator()
  {
    return new FailableAppenderator().interruptPush();
  }

  static Appenderator createPersistFailAppenderator()
  {
    return new FailableAppenderator().disablePersist();
  }

  static Appenderator createDropFailAppenderator()
  {
    return new FailableAppenderator().disableDrop();
  }

  private static class FailableAppenderator implements Appenderator
  {
    private final Map<SegmentIdentifier, List<InputRow>> rows = new HashMap<>();

    private boolean dropEnabled = true;
    private boolean persistEnabled = true;
    private boolean pushEnabled = true;
    private boolean interruptPush = false;

    private int numRows;

    public FailableAppenderator disableDrop()
    {
      dropEnabled = false;
      return this;
    }

    public FailableAppenderator disablePersist()
    {
      persistEnabled = false;
      return this;
    }

    public FailableAppenderator disablePush()
    {
      pushEnabled = false;
      interruptPush = false;
      return this;
    }

    public FailableAppenderator interruptPush()
    {
      pushEnabled = false;
      interruptPush = true;
      return this;
    }

    @Override
    public String getDataSource()
    {
      return null;
    }

    @Override
    public Object startJob()
    {
      return null;
    }

    @Override
    public int add(
        SegmentIdentifier identifier, InputRow row, Supplier<Committer> committerSupplier
    ) throws IndexSizeExceededException, SegmentNotWritableException
    {
      rows.computeIfAbsent(identifier, k -> new ArrayList<>()).add(row);
      return ++numRows;
    }

    @Override
    public List<SegmentIdentifier> getSegments()
    {
      return ImmutableList.copyOf(rows.keySet());
    }

    @Override
    public int getRowCount(SegmentIdentifier identifier)
    {
      final List<InputRow> rows = this.rows.get(identifier);
      if (rows != null) {
        return rows.size();
      } else {
        return 0;
      }
    }

    @Override
    public int getTotalRowCount()
    {
      return numRows;
    }

    @Override
    public void clear() throws InterruptedException
    {
      rows.clear();
    }

    @Override
    public ListenableFuture<?> drop(SegmentIdentifier identifier)
    {
      if (dropEnabled) {
        rows.remove(identifier);
        return Futures.immediateFuture(null);
      } else {
        return Futures.immediateFailedFuture(new ISE("Fail test while dropping segment[%s]", identifier));
      }
    }

    @Override
    public ListenableFuture<Object> persist(
        Collection<SegmentIdentifier> identifiers, Committer committer
    )
    {
      if (persistEnabled) {
        // do nothing
        return Futures.immediateFuture(committer.getMetadata());
      } else {
        return Futures.immediateFailedFuture(new ISE("Fail test while persisting segments[%s]", identifiers));
      }
    }

    @Override
    public ListenableFuture<SegmentsAndMetadata> push(
        Collection<SegmentIdentifier> identifiers, Committer committer
    )
    {
      if (pushEnabled) {
        final List<DataSegment> segments = identifiers.stream()
                                                      .map(
                                                          id -> new DataSegment(
                                                              id.getDataSource(),
                                                              id.getInterval(),
                                                              id.getVersion(),
                                                              ImmutableMap.of(),
                                                              ImmutableList.of(),
                                                              ImmutableList.of(),
                                                              id.getShardSpec(),
                                                              0,
                                                              0
                                                          )
                                                      )
                                                      .collect(Collectors.toList());
        return Futures.transform(
            persist(identifiers, committer),
            (Function<Object, SegmentsAndMetadata>) commitMetadata -> new SegmentsAndMetadata(segments, commitMetadata)
        );
      } else {
        if (interruptPush) {
          return new AbstractFuture<SegmentsAndMetadata>()
          {
            @Override
            public SegmentsAndMetadata get(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException, ExecutionException
            {
              throw new InterruptedException("Interrupt test while pushing segments");
            }

            @Override
            public SegmentsAndMetadata get() throws InterruptedException, ExecutionException
            {
              throw new InterruptedException("Interrupt test while pushing segments");
            }
          };
        } else {
          return Futures.immediateFailedFuture(new ISE("Fail test while pushing segments[%s]", identifiers));
        }
      }
    }

    @Override
    public void close()
    {

    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(
        Query<T> query, Iterable<Interval> intervals
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(
        Query<T> query, Iterable<SegmentDescriptor> specs
    )
    {
      throw new UnsupportedOperationException();
    }
  }
}
