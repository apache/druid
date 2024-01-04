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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.SegmentSchemasChangeRequest;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class BatchDataSegmentAnnouncerTest
{
  private static final String TEST_BASE_PATH = "/test";
  private static final String TEST_SEGMENTS_PATH = "/test/segments/id";
  private static final Joiner JOINER = Joiner.on("/");
  private static final int NUM_THREADS = 4;

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private ObjectMapper jsonMapper;
  private TestAnnouncer announcer;
  private SegmentReader segmentReader;
  private BatchDataSegmentAnnouncer segmentAnnouncer;
  private Set<DataSegment> testSegments;

  private final AtomicInteger maxBytesPerNode = new AtomicInteger(512 * 1024);
  private Boolean skipDimensionsAndMetrics;
  private Boolean skipLoadSpec;

  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(TEST_BASE_PATH);

    jsonMapper = TestHelper.makeJsonMapper();

    announcer = new TestAnnouncer(
        cf,
        Execs.directExecutor()
    );
    announcer.start();

    segmentReader = new SegmentReader(cf, jsonMapper);
    skipDimensionsAndMetrics = false;
    skipLoadSpec = false;
    segmentAnnouncer = new BatchDataSegmentAnnouncer(
        new DruidServerMetadata(
            "id",
            "host",
            null,
            Long.MAX_VALUE,
            ServerType.HISTORICAL,
            "tier",
            0
        ),
        new BatchDataSegmentAnnouncerConfig()
        {
          @Override
          public int getSegmentsPerNode()
          {
            return 50;
          }

          @Override
          public long getMaxBytesPerNode()
          {
            return maxBytesPerNode.get();
          }

          @Override
          public boolean isSkipDimensionsAndMetrics()
          {
            return skipDimensionsAndMetrics;
          }

          @Override
          public boolean isSkipLoadSpec()
          {
            return skipLoadSpec;
          }
        },
        new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return TEST_BASE_PATH;
          }
        },
        announcer,
        jsonMapper
    );

    testSegments = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      testSegments.add(makeSegment(i));
    }

    exec = Execs.multiThreaded(NUM_THREADS, "BatchDataSegmentAnnouncerTest-%d");
  }

  @After
  public void tearDown() throws Exception
  {
    announcer.stop();
    cf.close();
    testingCluster.stop();
    exec.shutdownNow();
  }

  @Test
  public void testSingleAnnounce() throws Exception
  {
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();
    DataSegment secondSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode));
      Assert.assertEquals(segments.iterator().next(), firstSegment);
    }

    segmentAnnouncer.announceSegment(secondSegment);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode));
      Assert.assertEquals(Sets.newHashSet(firstSegment, secondSegment), segments);
    }

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = segmentAnnouncer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    segmentAnnouncer.unannounceSegment(firstSegment);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode));
      Assert.assertEquals(segments.iterator().next(), secondSegment);
    }

    segmentAnnouncer.unannounceSegment(secondSegment);

    Assert.assertTrue(cf.getChildren().forPath(TEST_SEGMENTS_PATH).isEmpty());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        snapshot.getCounter()
    ).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(0, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());
  }

  @Test
  public void testSingleTombstoneAnnounce() throws Exception
  {
    DataSegment firstSegment = makeSegment(0, true);

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode));
      Assert.assertEquals(segments.iterator().next(), firstSegment);
    }

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = segmentAnnouncer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(1, snapshot.getCounter().getCounter());

    segmentAnnouncer.unannounceSegment(firstSegment);

    Assert.assertTrue(cf.getChildren().forPath(TEST_SEGMENTS_PATH).isEmpty());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        snapshot.getCounter()
    ).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        new ChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(0, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());
  }

  @Test
  public void testSkipDimensions() throws Exception
  {
    skipDimensionsAndMetrics = true;
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);

    for (String zNode : zNodes) {
      DataSegment announcedSegment = Iterables.getOnlyElement(segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode)));
      Assert.assertEquals(announcedSegment, firstSegment);
      Assert.assertTrue(announcedSegment.getDimensions().isEmpty());
      Assert.assertTrue(announcedSegment.getMetrics().isEmpty());
    }

    segmentAnnouncer.unannounceSegment(firstSegment);

    Assert.assertTrue(cf.getChildren().forPath(TEST_SEGMENTS_PATH).isEmpty());
  }

  @Test
  public void testSkipLoadSpec() throws Exception
  {
    skipLoadSpec = true;
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);

    for (String zNode : zNodes) {
      DataSegment announcedSegment = Iterables.getOnlyElement(segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode)));
      Assert.assertEquals(announcedSegment, firstSegment);
      Assert.assertNull(announcedSegment.getLoadSpec());
    }

    segmentAnnouncer.unannounceSegment(firstSegment);

    Assert.assertTrue(cf.getChildren().forPath(TEST_SEGMENTS_PATH).isEmpty());
  }

  @Test
  public void testSingleAnnounceManyTimes() throws Exception
  {
    int prevMax = maxBytesPerNode.get();
    maxBytesPerNode.set(2048);
    // each segment is about 348 bytes long and that makes 2048 / 348 = 5 segments included per node
    // so 100 segments makes 100 / 5 = 20 nodes
    try {
      for (DataSegment segment : testSegments) {
        segmentAnnouncer.announceSegment(segment);
      }
    }
    finally {
      maxBytesPerNode.set(prevMax);
    }

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);
    Assert.assertEquals(20, zNodes.size());

    Set<DataSegment> segments = Sets.newHashSet(testSegments);
    for (String zNode : zNodes) {
      for (DataSegment segment : segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode))) {
        Assert.assertTrue("Invalid segment " + segment, segments.remove(segment));
      }
    }
    Assert.assertTrue("Failed to find segments " + segments, segments.isEmpty());
  }

  @Test
  public void testBatchAnnounce() throws Exception
  {
    testBatchAnnounce(true);
  }

  @Test
  public void testMultipleBatchAnnounce() throws Exception
  {
    for (int i = 0; i < 10; i++) {
      testBatchAnnounce(false);
    }
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

    segmentAnnouncer.announceSegmentSchemas(
        taskId,
        new SegmentSchemas(Collections.singletonList(absoluteSchema1)),
        new SegmentSchemas(Collections.singletonList(absoluteSchema1)));

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot;

    snapshot = segmentAnnouncer.getSegmentChangesSince(
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
    segmentAnnouncer.announceSegmentSchemas(
        taskId,
        new SegmentSchemas(Collections.singletonList(absoluteSchema2)),
        new SegmentSchemas(Collections.singletonList(deltaSchema))
    );

    snapshot = segmentAnnouncer.getSegmentChangesSince(snapshot.getCounter()).get();

    Assert.assertEquals(
        deltaSchema,
        ((SegmentSchemasChangeRequest) snapshot.getRequests().get(0))
            .getSegmentSchemas()
            .getSegmentSchemaList()
            .get(0)
    );
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
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

  private void testBatchAnnounce(boolean testHistory) throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    List<String> zNodes = cf.getChildren().forPath(TEST_SEGMENTS_PATH);

    Assert.assertEquals(2, zNodes.size());

    Set<DataSegment> allSegments = new HashSet<>();
    for (String zNode : zNodes) {
      allSegments.addAll(segmentReader.read(JOINER.join(TEST_SEGMENTS_PATH, zNode)));
    }
    Assert.assertEquals(allSegments, testSegments);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot = null;

    if (testHistory) {
      snapshot = segmentAnnouncer.getSegmentChangesSince(
          new ChangeRequestHistory.Counter(-1, -1)
      ).get();
      Assert.assertEquals(testSegments.size(), snapshot.getRequests().size());
      Assert.assertEquals(testSegments.size(), snapshot.getCounter().getCounter());
    }

    segmentAnnouncer.unannounceSegments(testSegments);

    Assert.assertTrue(cf.getChildren().forPath(TEST_SEGMENTS_PATH).isEmpty());

    if (testHistory) {
      snapshot = segmentAnnouncer.getSegmentChangesSince(
          snapshot.getCounter()
      ).get();
      Assert.assertEquals(testSegments.size(), snapshot.getRequests().size());
      Assert.assertEquals(2 * testSegments.size(), snapshot.getCounter().getCounter());

      snapshot = segmentAnnouncer.getSegmentChangesSince(
          new ChangeRequestHistory.Counter(-1, -1)
      ).get();
      Assert.assertEquals(0, snapshot.getRequests().size());
      Assert.assertEquals(2 * testSegments.size(), snapshot.getCounter().getCounter());
    }
  }

  @Test(timeout = 5000L)
  public void testAnnounceSegmentsWithSameSegmentConcurrently() throws ExecutionException, InterruptedException
  {
    final List<Future> futures = new ArrayList<>(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(
          exec.submit(() -> {
            try {
              segmentAnnouncer.announceSegments(testSegments);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
      );
    }

    for (Future future : futures) {
      future.get();
    }

    // Announcing 100 segments requires 2 nodes because of maxBytesPerNode configuration.
    Assert.assertEquals(2, announcer.numPathAnnounced.size());
    for (ConcurrentHashMap<byte[], AtomicInteger> eachMap : announcer.numPathAnnounced.values()) {
      for (Entry<byte[], AtomicInteger> entry : eachMap.entrySet()) {
        Assert.assertEquals(1, entry.getValue().get());
      }
    }
  }

  @Test(timeout = 5000L)
  public void testAnnounceSegmentWithSameSegmentConcurrently() throws ExecutionException, InterruptedException
  {
    final List<Future> futures = new ArrayList<>(NUM_THREADS);

    final DataSegment segment1 = makeSegment(0);
    final DataSegment segment2 = makeSegment(1);
    final DataSegment segment3 = makeSegment(2);
    final DataSegment segment4 = makeSegment(3);

    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(
          exec.submit(() -> {
            try {
              segmentAnnouncer.announceSegment(segment1);
              segmentAnnouncer.announceSegment(segment2);
              segmentAnnouncer.announceSegment(segment3);
              segmentAnnouncer.announceSegment(segment4);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
      );
    }

    for (Future future : futures) {
      future.get();
    }

    Assert.assertEquals(1, announcer.numPathAnnounced.size());
    for (ConcurrentHashMap<byte[], AtomicInteger> eachMap : announcer.numPathAnnounced.values()) {
      for (Entry<byte[], AtomicInteger> entry : eachMap.entrySet()) {
        Assert.assertEquals(1, entry.getValue().get());
      }
    }
  }

  private DataSegment makeSegment(int offset, boolean isTombstone)
  {
    DataSegment.Builder builder = DataSegment.builder();
    builder.dataSource("foo")
           .interval(
               new Interval(
                   DateTimes.of("2013-01-01").plusDays(offset),
                   DateTimes.of("2013-01-02").plusDays(offset)
               )
           )
           .version(DateTimes.nowUtc().toString())
           .dimensions(ImmutableList.of("dim1", "dim2"))
           .metrics(ImmutableList.of("met1", "met2"))
           .loadSpec(ImmutableMap.of("type", "local"))
           .size(0);
    if (isTombstone) {
      builder.loadSpec(Collections.singletonMap("type", DataSegment.TOMBSTONE_LOADSPEC_TYPE));
    }

    return builder.build();
  }

  private DataSegment makeSegment(int offset)
  {
    return makeSegment(offset, false);
  }

  private static class SegmentReader
  {
    private final CuratorFramework cf;
    private final ObjectMapper jsonMapper;

    public SegmentReader(CuratorFramework cf, ObjectMapper jsonMapper)
    {
      this.cf = cf;
      this.jsonMapper = jsonMapper;
    }

    public Set<DataSegment> read(String path)
    {
      try {
        if (cf.checkExists().forPath(path) != null) {
          return jsonMapper.readValue(
              cf.getData().forPath(path), new TypeReference<Set<DataSegment>>()
              {
              }
          );
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return new HashSet<>();
    }
  }

  private static class TestAnnouncer extends Announcer
  {
    private final ConcurrentHashMap<String, ConcurrentHashMap<byte[], AtomicInteger>> numPathAnnounced = new ConcurrentHashMap<>();

    private TestAnnouncer(CuratorFramework curator, ExecutorService exec)
    {
      super(curator, exec);
    }

    @Override
    public void announce(String path, byte[] bytes, boolean removeParentIfCreated)
    {
      numPathAnnounced.computeIfAbsent(path, k -> new ConcurrentHashMap<>()).computeIfAbsent(bytes, k -> new AtomicInteger(0)).incrementAndGet();
      super.announce(path, bytes, removeParentIfCreated);
    }
  }
}
