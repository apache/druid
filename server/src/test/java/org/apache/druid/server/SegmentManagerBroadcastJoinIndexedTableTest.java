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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.join.BroadcastTableJoinableFactory;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.loading.BroadcastJoinableMMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SegmentManagerBroadcastJoinIndexedTableTest extends InitializedNullHandlingTest
{
  private static final String TABLE_NAME = "test";
  private static final String PREFIX = "j0";
  private static final JoinConditionAnalysis JOIN_CONDITION_ANALYSIS = JoinConditionAnalysis.forExpression(
      StringUtils.format("market == \"%s.market\"", PREFIX),
      PREFIX,
      ExprMacroTable.nil()
  );
  private static final Set<String> KEY_COLUMNS =
      ImmutableSet.of("market", "longNumericNull", "doubleNumericNull", "floatNumericNull", "partial_null_column");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private LocalDataSegmentPuller segmentPuller;
  private ObjectMapper objectMapper;
  private IndexIO indexIO;
  private File segmentCacheDir;
  private File segmentDeepStorageDir;
  private SegmentLoaderLocalCacheManager segmentLoader;
  private SegmentManager segmentManager;
  private BroadcastTableJoinableFactory joinableFactory;

  @Before
  public void setup() throws IOException
  {
    segmentPuller = new LocalDataSegmentPuller();
    objectMapper = new DefaultObjectMapper()
        .registerModule(new SegmentizerModule())
        .registerModule(
            new SimpleModule().registerSubtypes(new NamedType(LocalLoadSpec.class, "local"))
        );

    indexIO = new IndexIO(objectMapper, () -> 0);
    objectMapper.setInjectableValues(
        new InjectableValues.Std().addValue(LocalDataSegmentPuller.class, segmentPuller)
                                  .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
                                  .addValue(ObjectMapper.class.getName(), objectMapper)
                                  .addValue(IndexIO.class, indexIO)
    );
    segmentCacheDir = temporaryFolder.newFolder();
    segmentDeepStorageDir = temporaryFolder.newFolder();
    segmentLoader = new SegmentLoaderLocalCacheManager(
        indexIO,
        new SegmentLoaderConfig()
        {
          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return Collections.singletonList(
                new StorageLocationConfig(segmentCacheDir, null, null)
            );
          }
        },
        objectMapper
    );
    segmentManager = new SegmentManager(segmentLoader);
    joinableFactory = new BroadcastTableJoinableFactory(segmentManager);
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @After
  public void teardown() throws IOException
  {
    FileUtils.deleteDirectory(segmentCacheDir);
  }

  @Test
  public void testLoadIndexedTable() throws IOException, SegmentLoadingException
  {
    final DataSource dataSource = new GlobalTableDataSource(TABLE_NAME);
    Assert.assertFalse(joinableFactory.isDirectlyJoinable(dataSource));

    final String version = DateTimes.nowUtc().toString();
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");
    final String interval = "2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z";
    DataSegment segment = createSegment(data, interval, version);
    Assert.assertTrue(segmentManager.loadSegment(segment, false, SegmentLazyLoadFailCallback.NOOP));

    Assert.assertTrue(joinableFactory.isDirectlyJoinable(dataSource));
    Optional<Joinable> maybeJoinable = makeJoinable(dataSource);
    Assert.assertTrue(maybeJoinable.isPresent());
    Joinable joinable = maybeJoinable.get();
    // cardinality currently tied to number of rows,
    Assert.assertEquals(1210, joinable.getCardinality("market"));
    Assert.assertEquals(1210, joinable.getCardinality("placement"));
    Assert.assertEquals(
        Optional.of(ImmutableSet.of("preferred")),
        joinable.getCorrelatedColumnValues(
            "market",
            "spot",
            "placement",
            Long.MAX_VALUE,
            false
        )
    );
    Optional<byte[]> bytes = joinableFactory.computeJoinCacheKey(dataSource, JOIN_CONDITION_ANALYSIS);
    Assert.assertTrue(bytes.isPresent());
    assertSegmentIdEquals(segment.getId(), bytes.get());

    // dropping the segment should make the table no longer available
    segmentManager.dropSegment(segment);

    maybeJoinable = makeJoinable(dataSource);

    Assert.assertFalse(maybeJoinable.isPresent());

    bytes = joinableFactory.computeJoinCacheKey(dataSource, JOIN_CONDITION_ANALYSIS);
    Assert.assertFalse(bytes.isPresent());
  }

  @Test
  public void testLoadMultipleIndexedTableOverwrite() throws IOException, SegmentLoadingException
  {
    final DataSource dataSource = new GlobalTableDataSource(TABLE_NAME);
    Assert.assertFalse(joinableFactory.isDirectlyJoinable(dataSource));

    // larger interval overwrites smaller interval
    final String version = DateTimes.nowUtc().toString();
    final String version2 = DateTimes.nowUtc().plus(1000L).toString();
    final String interval = "2011-01-12T00:00:00.000Z/2011-03-28T00:00:00.000Z";
    final String interval2 = "2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z";
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv.top");
    IncrementalIndex data2 = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv.bottom");
    DataSegment segment1 = createSegment(data, interval, version);
    DataSegment segment2 = createSegment(data2, interval2, version2);
    Assert.assertTrue(segmentManager.loadSegment(segment1, false, SegmentLazyLoadFailCallback.NOOP));
    Assert.assertTrue(segmentManager.loadSegment(segment2, false, SegmentLazyLoadFailCallback.NOOP));

    Assert.assertTrue(joinableFactory.isDirectlyJoinable(dataSource));
    Optional<Joinable> maybeJoinable = makeJoinable(dataSource);
    Assert.assertTrue(maybeJoinable.isPresent());

    Joinable joinable = maybeJoinable.get();
    // cardinality currently tied to number of rows,
    Assert.assertEquals(733, joinable.getCardinality("market"));
    Assert.assertEquals(733, joinable.getCardinality("placement"));
    Assert.assertEquals(
        Optional.of(ImmutableSet.of("preferred")),
        joinable.getCorrelatedColumnValues(
            "market",
            "spot",
            "placement",
            Long.MAX_VALUE,
            false
        )
    );
    Optional<byte[]> cacheKey = joinableFactory.computeJoinCacheKey(dataSource, JOIN_CONDITION_ANALYSIS);
    Assert.assertTrue(cacheKey.isPresent());
    assertSegmentIdEquals(segment2.getId(), cacheKey.get());

    segmentManager.dropSegment(segment2);

    // if new segment is dropped for some reason that probably never happens, old table should still exist..
    maybeJoinable = makeJoinable(dataSource);
    Assert.assertTrue(maybeJoinable.isPresent());

    joinable = maybeJoinable.get();
    // cardinality currently tied to number of rows,
    Assert.assertEquals(478, joinable.getCardinality("market"));
    Assert.assertEquals(478, joinable.getCardinality("placement"));
    Assert.assertEquals(
        Optional.of(ImmutableSet.of("preferred")),
        joinable.getCorrelatedColumnValues(
            "market",
            "spot",
            "placement",
            Long.MAX_VALUE,
            false
        )
    );
    cacheKey = joinableFactory.computeJoinCacheKey(dataSource, JOIN_CONDITION_ANALYSIS);
    Assert.assertTrue(cacheKey.isPresent());
    assertSegmentIdEquals(segment1.getId(), cacheKey.get());
  }


  @Test
  public void testLoadMultipleIndexedTable() throws IOException, SegmentLoadingException
  {
    final DataSource dataSource = new GlobalTableDataSource(TABLE_NAME);
    Assert.assertFalse(joinableFactory.isDirectlyJoinable(dataSource));

    final String version = DateTimes.nowUtc().toString();
    final String version2 = DateTimes.nowUtc().plus(1000L).toString();
    final String interval = "2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z";
    final String interval2 = "2011-01-12T00:00:00.000Z/2011-03-28T00:00:00.000Z";
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv.bottom");
    IncrementalIndex data2 = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv.top");
    Assert.assertTrue(segmentManager.loadSegment(createSegment(data, interval, version), false, SegmentLazyLoadFailCallback.NOOP));
    Assert.assertTrue(joinableFactory.isDirectlyJoinable(dataSource));

    Optional<Joinable> maybeJoinable = makeJoinable(dataSource);
    Assert.assertTrue(maybeJoinable.isPresent());

    Joinable joinable = maybeJoinable.get();
    // cardinality currently tied to number of rows,
    Assert.assertEquals(733, joinable.getCardinality("market"));
    Assert.assertEquals(733, joinable.getCardinality("placement"));
    Assert.assertEquals(
        Optional.of(ImmutableSet.of("preferred")),
        joinable.getCorrelatedColumnValues(
            "market",
            "spot",
            "placement",
            Long.MAX_VALUE,
            false
        )
    );

    // add another segment with smaller interval, only partially overshadows so there will be 2 segments in timeline
    Assert.assertTrue(segmentManager.loadSegment(createSegment(data2, interval2, version2), false, SegmentLazyLoadFailCallback.NOOP));


    expectedException.expect(ISE.class);
    expectedException.expectMessage(
        StringUtils.format(
            "Currently only single segment datasources are supported for broadcast joins, dataSource[%s] has multiple segments. Reingest the data so that it is entirely contained within a single segment to use in JOIN queries.",
            TABLE_NAME
        )
    );
    // this will explode because datasource has multiple segments which is an invalid state for the joinable factory
    makeJoinable(dataSource);
  }

  @Test
  public void emptyCacheKeyForUnsupportedCondition()
  {
    final DataSource dataSource = new GlobalTableDataSource(TABLE_NAME);
    JoinConditionAnalysis condition = EasyMock.mock(JoinConditionAnalysis.class);
    EasyMock.expect(condition.canHashJoin()).andReturn(false);
    EasyMock.replay(condition);
    Assert.assertNull(joinableFactory.build(dataSource, condition).orElse(null));
  }

  private Optional<Joinable> makeJoinable(DataSource dataSource)
  {
    return joinableFactory.build(dataSource, JOIN_CONDITION_ANALYSIS);
  }

  private DataSegment createSegment(IncrementalIndex data, String interval, String version) throws IOException
  {
    final DataSegment tmpSegment = new DataSegment(
        TABLE_NAME,
        Intervals.of(interval),
        version,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        new NumberedShardSpec(0, 0),
        9,
        100
    );
    final String storageDir = DataSegmentPusher.getDefaultStorageDir(tmpSegment, false);
    final File segmentDir = new File(segmentDeepStorageDir, storageDir);
    org.apache.commons.io.FileUtils.forceMkdir(segmentDir);

    IndexMerger indexMerger =
        new IndexMergerV9(objectMapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    SegmentizerFactory factory = new BroadcastJoinableMMappedQueryableSegmentizerFactory(indexIO, KEY_COLUMNS);

    indexMerger.persist(
        data,
        Intervals.of(interval),
        segmentDir,
        new IndexSpec(
            null,
            null,
            null,
            null,
            factory
        ),
        null
    );
    final File factoryJson = new File(segmentDir, "factory.json");
    objectMapper.writeValue(factoryJson, factory);
    return tmpSegment.withLoadSpec(
        ImmutableMap.of("type", "local", "path", segmentDir.getAbsolutePath())
    );
  }

  private void assertSegmentIdEquals(SegmentId id, byte[] bytes)
  {
    // Call byteBuffer.get to skip the type keys
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.get(); // skip the cache key prefix
    byteBuffer.get();
    long start = byteBuffer.getLong();
    byteBuffer.get();
    long end = byteBuffer.getLong();
    byteBuffer.get();
    String version = StringUtils.fromUtf8(byteBuffer, StringUtils.estimatedBinaryLengthAsUTF8(id.getVersion()));
    byteBuffer.get();
    String dataSource = StringUtils.fromUtf8(byteBuffer, StringUtils.estimatedBinaryLengthAsUTF8(id.getDataSource()));
    byteBuffer.get();
    int partition = byteBuffer.getInt();
    Assert.assertEquals(id, SegmentId.of(dataSource, Intervals.utc(start, end), version, partition));
  }
}
