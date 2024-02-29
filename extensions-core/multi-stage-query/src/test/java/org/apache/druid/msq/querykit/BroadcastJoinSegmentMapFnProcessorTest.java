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

package org.apache.druid.msq.querykit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.error.BroadcastTablesTooLargeFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BroadcastJoinSegmentMapFnProcessorTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StorageAdapter adapter;
  private File testDataFile1;
  private File testDataFile2;
  private FrameReader frameReader1;
  private FrameReader frameReader2;

  @Before
  public void setUp() throws IOException
  {
    final ArenaMemoryAllocator allocator = ArenaMemoryAllocator.createOnHeap(10_000);
    adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

    // File 1: the entire test dataset.
    testDataFile1 = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .allocator(allocator)
                            .frames(),
        temporaryFolder.newFile()
    );

    // File 2: just two rows.
    testDataFile2 = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .allocator(allocator)
                            .maxRowsPerFrame(1)
                            .frames()
                            .limit(2),
        temporaryFolder.newFile()
    );

    frameReader1 = FrameReader.create(adapter.getRowSignature());
    frameReader2 = FrameReader.create(adapter.getRowSignature());
  }

  @Test
  public void testBuildTableAndInlineData() throws IOException
  {
    final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
    sideStageChannelNumberMap.put(3, 1);
    sideStageChannelNumberMap.put(4, 2);

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    channels.add(new ExplodingReadableFrameChannel());
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile1, null)));
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile2, null)));

    final List<FrameReader> channelReaders = new ArrayList<>();
    channelReaders.add(null);
    channelReaders.add(frameReader1);
    channelReaders.add(frameReader2);

    final BroadcastJoinSegmentMapFnProcessor broadcastJoinReader = new BroadcastJoinSegmentMapFnProcessor(
        null /* Query; not used for the methods we're testing */,
        sideStageChannelNumberMap,
        channels,
        channelReaders,
        25_000_000L // High enough memory limit that we won't hit it
    );

    Assert.assertEquals(ImmutableSet.of(1, 2), broadcastJoinReader.getSideChannelNumbers());

    boolean doneReading = false;
    while (!doneReading) {
      final IntSet readableInputs = new IntOpenHashSet();
      for (int i = 1; i < channels.size(); i++) {
        readableInputs.add(i); // Frame file channels are always ready, so this is OK.
      }
      doneReading = broadcastJoinReader.buildBroadcastTablesIncrementally(readableInputs);
    }

    Assert.assertTrue(channels.get(1).isFinished());
    Assert.assertTrue(channels.get(2).isFinished());

    Assert.assertEquals(
        new InputNumberDataSource(0),
        broadcastJoinReader.inlineChannelData(new InputNumberDataSource(0))
    );

    Assert.assertEquals(
        new InputNumberDataSource(1),
        broadcastJoinReader.inlineChannelData(new InputNumberDataSource(1))
    );

    Assert.assertEquals(
        new InputNumberDataSource(2),
        broadcastJoinReader.inlineChannelData(new InputNumberDataSource(2))
    );

    final List<Object[]> rowsFromStage3 =
        ((InlineDataSource) broadcastJoinReader.inlineChannelData(new InputNumberDataSource(3))).getRowsAsList();
    Assert.assertEquals(1209, rowsFromStage3.size());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        Sequences.simple(rowsFromStage3.stream().map(Arrays::asList).collect(Collectors.toList()))
    );

    final List<Object[]> rowsFromStage4 =
        ((InlineDataSource) broadcastJoinReader.inlineChannelData(new InputNumberDataSource(4))).getRowsAsList();
    Assert.assertEquals(2, rowsFromStage4.size());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false).limit(2),
        Sequences.simple(rowsFromStage4.stream().map(Arrays::asList).collect(Collectors.toList()))
    );

    final DataSource inlinedJoinDataSource = broadcastJoinReader.inlineChannelData(
        JoinDataSource.create(
            new InputNumberDataSource(0),
            new InputNumberDataSource(4),
            "j.",
            JoinConditionAnalysis.forExpression("x == \"j.x\"", "j.", ExprMacroTable.nil()),
            JoinType.INNER,
            null,
            null
        )
    );

    MatcherAssert.assertThat(
        ((JoinDataSource) inlinedJoinDataSource).getRight(),
        CoreMatchers.instanceOf(InlineDataSource.class)
    );

    Assert.assertEquals(
        2,
        ((InlineDataSource) ((JoinDataSource) inlinedJoinDataSource).getRight()).getRowsAsList().size()
    );
  }

  @Test
  public void testBuildTableMemoryLimit() throws IOException
  {
    final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
    sideStageChannelNumberMap.put(0, 0);

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile1, ByteTracker.unboundedTracker())));

    final List<FrameReader> channelReaders = new ArrayList<>();
    channelReaders.add(frameReader1);

    final BroadcastJoinSegmentMapFnProcessor broadcastJoinHelper = new BroadcastJoinSegmentMapFnProcessor(
        null /* Query; not used for the methods we're testing */,
        sideStageChannelNumberMap,
        channels,
        channelReaders,
        100_000 // Low memory limit; we will hit this
    );

    Assert.assertEquals(ImmutableSet.of(0), broadcastJoinHelper.getSideChannelNumbers());

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> {
          boolean doneReading = false;
          while (!doneReading) {
            final IntSet readableInputs = new IntOpenHashSet(new int[]{0});
            doneReading = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);
          }
        }
    );

    Assert.assertEquals(new BroadcastTablesTooLargeFault(100_000, null), e.getFault());
  }

  /**
   * Like {@link #testBuildTableMemoryLimit()}, but with {@link JoinAlgorithm#SORT_MERGE} configured, so we can
   * verify we get a better error message.
   */
  @Test
  public void testBuildTableMemoryLimitWithSortMergeConfigured() throws IOException
  {
    final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
    sideStageChannelNumberMap.put(0, 0);

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile1, ByteTracker.unboundedTracker())));

    final List<FrameReader> channelReaders = new ArrayList<>();
    channelReaders.add(frameReader1);

    // Query: used only to retrieve configured join from context
    final Query<?> mockQuery = EasyMock.mock(Query.class);
    EasyMock.expect(mockQuery.context()).andReturn(
        QueryContext.of(
            ImmutableMap.of(
                PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                JoinAlgorithm.SORT_MERGE.getId()
            )
        )
    );
    EasyMock.replay(mockQuery);
    final BroadcastJoinSegmentMapFnProcessor broadcastJoinHelper = new BroadcastJoinSegmentMapFnProcessor(
        mockQuery,
        sideStageChannelNumberMap,
        channels,
        channelReaders,
        100_000 // Low memory limit; we will hit this
    );

    Assert.assertEquals(ImmutableSet.of(0), broadcastJoinHelper.getSideChannelNumbers());

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> {
          boolean doneReading = false;
          while (!doneReading) {
            final IntSet readableInputs = new IntOpenHashSet(new int[]{0});
            doneReading = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);
          }
        }
    );

    Assert.assertEquals(new BroadcastTablesTooLargeFault(100_000, JoinAlgorithm.SORT_MERGE), e.getFault());
    EasyMock.verify(mockQuery);
  }

  /**
   * Throws an error on every method call. Useful for ensuring that a channel is *not* read.
   */
  private static class ExplodingReadableFrameChannel implements ReadableFrameChannel
  {
    @Override
    public boolean isFinished()
    {
      throw new IllegalStateException();
    }

    @Override
    public boolean canRead()
    {
      throw new IllegalStateException();
    }

    @Override
    public Frame read()
    {
      throw new IllegalStateException();
    }

    @Override
    public ListenableFuture<?> readabilityFuture()
    {
      throw new IllegalStateException();
    }

    @Override
    public void close()
    {
      throw new IllegalStateException();
    }
  }
}
