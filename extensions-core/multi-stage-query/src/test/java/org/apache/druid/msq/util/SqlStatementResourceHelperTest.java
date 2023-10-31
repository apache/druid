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

package org.apache.druid.msq.util;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.Frame;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class SqlStatementResourceHelperTest
{

  private static final Logger log = new Logger(SqlStatementResourceHelperTest.class);

  @Test
  public void testDistinctPartitionsOnEachWorker()
  {
    CounterSnapshotsTree counterSnapshots = new CounterSnapshotsTree();
    ChannelCounters worker0 = createChannelCounters(new int[]{0, 3, 6});
    ChannelCounters worker1 = createChannelCounters(new int[]{1, 4, 4, 7, 9, 10, 13});
    ChannelCounters worker2 = createChannelCounters(new int[]{2, 5, 8, 11, 14});

    counterSnapshots.put(0, 0, new CounterSnapshots(ImmutableMap.of("output", worker0.snapshot())));
    counterSnapshots.put(0, 1, new CounterSnapshots(ImmutableMap.of("output", worker1.snapshot())));
    counterSnapshots.put(0, 2, new CounterSnapshots(ImmutableMap.of("output", worker2.snapshot())));

    MSQTaskReportPayload payload = new MSQTaskReportPayload(new MSQStatusReport(
        TaskState.SUCCESS,
        null,
        new ArrayDeque<>(),
        null,
        0,
        new HashMap<>(),
        1,
        2,
        null
    ), MSQStagesReport.create(
        MSQTaskReportTest.QUERY_DEFINITION,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(0, 3),
        ImmutableMap.of(0, 15)
    ), counterSnapshots, null);

    Optional<List<PageInformation>> pages = SqlStatementResourceHelper.populatePageList(
        payload,
        DurableStorageMSQDestination.instance()
    );
    validatePages(pages.get(), createValidationMap(worker0, worker1, worker2));
  }

  @Test
  public void testOnePartitionOnEachWorker()
  {
    CounterSnapshotsTree counterSnapshots = new CounterSnapshotsTree();
    ChannelCounters worker0 = createChannelCounters(new int[]{0});
    ChannelCounters worker1 = createChannelCounters(new int[]{1});
    ChannelCounters worker2 = createChannelCounters(new int[]{2});
    ChannelCounters worker3 = createChannelCounters(new int[]{4});

    counterSnapshots.put(0, 0, new CounterSnapshots(ImmutableMap.of("output", worker0.snapshot())));
    counterSnapshots.put(0, 1, new CounterSnapshots(ImmutableMap.of("output", worker1.snapshot())));
    counterSnapshots.put(0, 2, new CounterSnapshots(ImmutableMap.of("output", worker2.snapshot())));
    counterSnapshots.put(0, 3, new CounterSnapshots(ImmutableMap.of("output", worker3.snapshot())));

    MSQTaskReportPayload payload = new MSQTaskReportPayload(new MSQStatusReport(
        TaskState.SUCCESS,
        null,
        new ArrayDeque<>(),
        null,
        0,
        new HashMap<>(),
        1,
        2,
        null
    ), MSQStagesReport.create(
        MSQTaskReportTest.QUERY_DEFINITION,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(0, 4),
        ImmutableMap.of(0, 4)
    ), counterSnapshots, null);

    Optional<List<PageInformation>> pages = SqlStatementResourceHelper.populatePageList(
        payload,
        DurableStorageMSQDestination.instance()
    );
    validatePages(pages.get(), createValidationMap(worker0, worker1, worker2));
  }


  @Test
  public void testCommonPartitionsOnEachWorker()
  {
    CounterSnapshotsTree counterSnapshots = new CounterSnapshotsTree();
    ChannelCounters worker0 = createChannelCounters(new int[]{0, 1, 2, 3, 8, 9});
    ChannelCounters worker1 = createChannelCounters(new int[]{1, 4, 12});
    ChannelCounters worker2 = createChannelCounters(new int[]{20});
    ChannelCounters worker3 = createChannelCounters(new int[]{2, 2, 5, 6, 7, 9, 15});

    counterSnapshots.put(0, 0, new CounterSnapshots(ImmutableMap.of("output", worker0.snapshot())));
    counterSnapshots.put(0, 1, new CounterSnapshots(ImmutableMap.of("output", worker1.snapshot())));
    counterSnapshots.put(0, 2, new CounterSnapshots(ImmutableMap.of("output", worker2.snapshot())));
    counterSnapshots.put(0, 3, new CounterSnapshots(ImmutableMap.of("output", worker3.snapshot())));

    MSQTaskReportPayload payload = new MSQTaskReportPayload(new MSQStatusReport(
        TaskState.SUCCESS,
        null,
        new ArrayDeque<>(),
        null,
        0,
        new HashMap<>(),
        1,
        2,
        null
    ), MSQStagesReport.create(
        MSQTaskReportTest.QUERY_DEFINITION,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(0, 4),
        ImmutableMap.of(0, 21)
    ), counterSnapshots, null);

    Optional<List<PageInformation>> pages =
        SqlStatementResourceHelper.populatePageList(payload, DurableStorageMSQDestination.instance());
    validatePages(pages.get(), createValidationMap(worker0, worker1, worker2, worker3));
  }


  @Test
  public void testNullChannelCounters()
  {
    CounterSnapshotsTree counterSnapshots = new CounterSnapshotsTree();
    ChannelCounters worker0 = createChannelCounters(new int[0]);
    ChannelCounters worker1 = createChannelCounters(new int[]{1, 4, 12});
    ChannelCounters worker2 = createChannelCounters(new int[]{20});
    ChannelCounters worker3 = createChannelCounters(new int[]{2, 2, 5, 6, 7, 9, 15});

    counterSnapshots.put(0, 0, new CounterSnapshots(new HashMap<>()));
    counterSnapshots.put(0, 1, new CounterSnapshots(ImmutableMap.of("output", worker1.snapshot())));
    counterSnapshots.put(0, 2, new CounterSnapshots(ImmutableMap.of("output", worker2.snapshot())));
    counterSnapshots.put(0, 3, new CounterSnapshots(ImmutableMap.of("output", worker3.snapshot())));

    MSQTaskReportPayload payload = new MSQTaskReportPayload(new MSQStatusReport(
        TaskState.SUCCESS,
        null,
        new ArrayDeque<>(),
        null,
        0,
        new HashMap<>(),
        1,
        2,
        null
    ), MSQStagesReport.create(
        MSQTaskReportTest.QUERY_DEFINITION,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(0, 4),
        ImmutableMap.of(0, 21)
    ), counterSnapshots, null);

    Optional<List<PageInformation>> pages = SqlStatementResourceHelper.populatePageList(
        payload,
        DurableStorageMSQDestination.instance()
    );
    validatePages(pages.get(), createValidationMap(worker0, worker1, worker2, worker3));
  }


  @Test
  public void testConsecutivePartitionsOnEachWorker()
  {
    CounterSnapshotsTree counterSnapshots = new CounterSnapshotsTree();
    ChannelCounters worker0 = createChannelCounters(new int[]{0, 1, 2});
    ChannelCounters worker1 = createChannelCounters(new int[]{3, 4, 5});
    ChannelCounters worker2 = createChannelCounters(new int[]{6, 7, 8});
    ChannelCounters worker3 = createChannelCounters(new int[]{9, 10, 11, 12});

    counterSnapshots.put(0, 0, new CounterSnapshots(ImmutableMap.of("output", worker0.snapshot())));
    counterSnapshots.put(0, 1, new CounterSnapshots(ImmutableMap.of("output", worker1.snapshot())));
    counterSnapshots.put(0, 2, new CounterSnapshots(ImmutableMap.of("output", worker2.snapshot())));
    counterSnapshots.put(0, 3, new CounterSnapshots(ImmutableMap.of("output", worker3.snapshot())));

    MSQTaskReportPayload payload = new MSQTaskReportPayload(new MSQStatusReport(
        TaskState.SUCCESS,
        null,
        new ArrayDeque<>(),
        null,
        0,
        new HashMap<>(),
        1,
        2,
        null
    ), MSQStagesReport.create(
        MSQTaskReportTest.QUERY_DEFINITION,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(0, 4),
        ImmutableMap.of(0, 13)
    ), counterSnapshots, null);

    Optional<List<PageInformation>> pages = SqlStatementResourceHelper.populatePageList(
        payload,
        DurableStorageMSQDestination.instance()
    );
    validatePages(pages.get(), createValidationMap(worker0, worker1, worker2, worker3));
  }


  private void validatePages(
      List<PageInformation> pageList,
      Map<Integer, Map<Integer, Pair<Long, Long>>> partitionToWorkerToRowsBytes
  )
  {
    int currentPage = 0;
    for (Map.Entry<Integer, Map<Integer, Pair<Long, Long>>> partitionWorker : partitionToWorkerToRowsBytes.entrySet()) {
      for (Map.Entry<Integer, Pair<Long, Long>> workerRowsBytes : partitionWorker.getValue().entrySet()) {
        PageInformation pageInformation = pageList.get(currentPage);
        Assert.assertEquals(currentPage, pageInformation.getId());
        Assert.assertEquals(workerRowsBytes.getValue().lhs, pageInformation.getNumRows());
        Assert.assertEquals(workerRowsBytes.getValue().rhs, pageInformation.getSizeInBytes());
        Assert.assertEquals(partitionWorker.getKey(), pageInformation.getPartition());
        Assert.assertEquals(workerRowsBytes.getKey(), pageInformation.getWorker());
        log.debug(pageInformation.toString());
        currentPage++;
      }
    }
    Assert.assertEquals(currentPage, pageList.size());
  }

  private Map<Integer, Map<Integer, Pair<Long, Long>>> createValidationMap(
      ChannelCounters... workers
  )
  {
    if (workers == null || workers.length == 0) {
      return new HashMap<>();
    } else {
      Map<Integer, Map<Integer, Pair<Long, Long>>> partitionToWorkerToRowsBytes = new TreeMap<>();
      for (int worker = 0; worker < workers.length; worker++) {
        ChannelCounters.Snapshot workerCounter = workers[worker].snapshot();
        for (int partition = 0; workerCounter != null && partition < workerCounter.getRows().length; partition++) {
          Map<Integer, Pair<Long, Long>> workerMap = partitionToWorkerToRowsBytes.computeIfAbsent(
              partition,
              k -> new TreeMap<>()
          );

          if (workerCounter.getRows()[partition] != 0) {
            workerMap.put(
                worker,
                new Pair<>(
                    workerCounter.getRows()[partition],
                    workerCounter.getBytes()[partition]
                )
            );
          }

        }
      }
      return partitionToWorkerToRowsBytes;
    }
  }


  private ChannelCounters createChannelCounters(int[] partitions)
  {
    if (partitions == null || partitions.length == 0) {
      return new ChannelCounters();
    }
    ChannelCounters channelCounters = new ChannelCounters();
    int prev = -1;
    for (int current : partitions) {
      if (prev > current) {
        throw new IllegalArgumentException("Channel numbers should be in increasing order");
      }
      channelCounters.addFrame(current, createFrame(current * 10 + 1, 100L));
      prev = current;
    }
    return channelCounters;
  }


  private Frame createFrame(int numRows, long numBytes)
  {
    Frame frame = EasyMock.mock(Frame.class);
    EasyMock.expect(frame.numRows()).andReturn(numRows).anyTimes();
    EasyMock.expect(frame.numBytes()).andReturn(numBytes).anyTimes();
    EasyMock.replay(frame);
    return frame;
  }
}
