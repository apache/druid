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

package org.apache.druid.server.coordination.coordination;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentSchemasChangeRequest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BatchDataSegmentAnnouncerTest
{
  private static final int NUM_THREADS = 4;

  private BatchDataSegmentAnnouncer announcer;
  private Set<DataSegment> testSegments;
  private ExecutorService exec;

  @Before
  public void setUp()
  {
    announcer = new BatchDataSegmentAnnouncer();
    testSegments = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      testSegments.add(makeSegment(i));
    }
    exec = Execs.multiThreaded(NUM_THREADS, "BatchDataSegmentAnnouncerTest-%d");
  }

  @After
  public void tearDown()
  {
    announcer.stop();
    exec.shutdownNow();
  }

  @Test
  public void testAnnounceAndUnannounceProduceChangeHistory() throws Exception
  {
    DataSegment segmentA = makeSegment(0);
    DataSegment segmentB = makeSegment(1);

    announcer.announceSegment(segmentA);
    announcer.announceSegment(segmentB);
    announcer.unannounceSegment(segmentA);

    // When the counter is negative, getSegmentChangesSince returns the current snapshot of
    // announced segments, which at this point is just segmentB.
    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(new ChangeRequestHistory.Counter(-1, -1)).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertTrue(snapshot.getRequests().get(0) instanceof SegmentChangeRequestLoad);
  }

  @Test
  public void testAnnounceSegmentsBatchesChangeRequests() throws Exception
  {
    List<DataSegment> segments = ImmutableList.of(makeSegment(0), makeSegment(1), makeSegment(2));

    announcer.announceSegments(segments);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(segments.size(), snapshot.getRequests().size());
    for (DataSegmentChangeRequest request : snapshot.getRequests()) {
      Assert.assertTrue(request instanceof SegmentChangeRequestLoad);
    }
  }

  @Test
  public void testUnannounceAfterAnnounceProducesDropRequest() throws Exception
  {
    DataSegment segment = makeSegment(0);

    announcer.announceSegment(segment);
    announcer.unannounceSegment(segment);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertTrue(snapshot.getRequests().get(0) instanceof SegmentChangeRequestLoad);
    Assert.assertTrue(snapshot.getRequests().get(1) instanceof SegmentChangeRequestDrop);
  }

  @Test
  public void testDuplicateAnnouncementIsIgnored() throws Exception
  {
    DataSegment segment = makeSegment(0);

    announcer.announceSegment(segment);
    announcer.announceSegment(segment);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
  }

  @Test
  public void testSchemaAnnounce() throws Exception
  {
    String dataSource = "foo";
    String segmentId = "id";
    String taskId = "t1";
    SegmentSchemas.SegmentSchema absoluteSchema1 =
        new SegmentSchemas.SegmentSchema(
            dataSource,
            segmentId,
            false,
            20,
            ImmutableList.of("dim1", "dim2"),
            Collections.emptyList(),
            ImmutableMap.of("dim1", ColumnType.STRING, "dim2", ColumnType.STRING)
        );


    SegmentSchemas.SegmentSchema absoluteSchema2 =
        new SegmentSchemas.SegmentSchema(
            dataSource,
            segmentId,
            false,
            40,
            ImmutableList.of("dim1", "dim2", "dim3"),
            ImmutableList.of(),
            ImmutableMap.of("dim1", ColumnType.UNKNOWN_COMPLEX, "dim2", ColumnType.STRING, "dim3", ColumnType.STRING)
        );

    SegmentSchemas.SegmentSchema deltaSchema =
        new SegmentSchemas.SegmentSchema(
            dataSource,
            segmentId,
            true,
            40,
            ImmutableList.of("dim3"),
            ImmutableList.of("dim1"),
            ImmutableMap.of("dim1", ColumnType.UNKNOWN_COMPLEX, "dim3", ColumnType.STRING)
        );

    announcer.announceSegmentSchemas(
        taskId,
        new SegmentSchemas(Collections.singletonList(absoluteSchema1)),
        new SegmentSchemas(Collections.singletonList(absoluteSchema1))
    );

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot;

    snapshot = announcer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(1, snapshot.getCounter().getCounter());

    Assert.assertEquals(
        absoluteSchema1,
        ((SegmentSchemasChangeRequest) snapshot.getRequests().get(0))
            .getSegmentSchemas()
            .getSegmentSchemaList()
            .get(0)
    );
    announcer.announceSegmentSchemas(
        taskId,
        new SegmentSchemas(Collections.singletonList(absoluteSchema2)),
        new SegmentSchemas(Collections.singletonList(deltaSchema))
    );

    snapshot = announcer.getSegmentChangesSince(snapshot.getCounter()).get();

    Assert.assertEquals(
        deltaSchema,
        ((SegmentSchemasChangeRequest) snapshot.getRequests().get(0))
            .getSegmentSchemas()
            .getSegmentSchemaList()
            .get(0)
    );
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    snapshot = announcer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(
        absoluteSchema2,
        ((SegmentSchemasChangeRequest) snapshot.getRequests().get(0))
            .getSegmentSchemas()
            .getSegmentSchemaList()
            .get(0)
    );
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());
  }

  @Test(timeout = 5000L)
  public void testAnnounceSegmentsWithSameSegmentConcurrently() throws ExecutionException, InterruptedException
  {
    final List<Future<?>> futures = new ArrayList<>(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(exec.submit(() -> announcer.announceSegments(testSegments)));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(testSegments.size(), snapshot.getRequests().size());
  }

  @Test(timeout = 5000L)
  public void testAnnounceSegmentWithSameSegmentConcurrently() throws ExecutionException, InterruptedException
  {
    final List<Future<?>> futures = new ArrayList<>(NUM_THREADS);

    final DataSegment segment1 = makeSegment(0);
    final DataSegment segment2 = makeSegment(1);
    final DataSegment segment3 = makeSegment(2);
    final DataSegment segment4 = makeSegment(3);

    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(
          exec.submit(() -> {
            announcer.announceSegment(segment1);
            announcer.announceSegment(segment2);
            announcer.announceSegment(segment3);
            announcer.announceSegment(segment4);
          })
      );
    }

    for (Future<?> future : futures) {
      future.get();
    }

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(4, snapshot.getRequests().size());
  }

  private static DataSegment makeSegment(int offset)
  {
    Interval interval = new Interval(
        DateTimes.of("2013-01-01").plusDays(offset),
        DateTimes.of("2013-01-02").plusDays(offset)
    );
    SegmentId segmentId = SegmentId.of("foo", interval, DateTimes.nowUtc().toString(), null);
    return DataSegment.builder(segmentId)
                      .loadSpec(ImmutableMap.of("type", "local"))
                      .dimensions(ImmutableList.of("dim1", "dim2"))
                      .metrics(ImmutableList.of("met1", "met2"))
                      .shardSpec(NoneShardSpec.instance())
                      .size(0)
                      .build();
  }
}
