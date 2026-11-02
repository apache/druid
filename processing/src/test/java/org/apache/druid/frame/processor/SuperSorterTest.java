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

package org.apache.druid.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.processor.test.AlwaysAsyncPartitionedReadableFrameChannel;
import org.apache.druid.frame.processor.test.AlwaysAsyncReadableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.LongStream;

public class SuperSorterTest
{
  private static final Logger log = new Logger(SuperSorterTest.class);

  /**
   * Non-parameterized test cases for specific scenarios.
   */
  public static class NonParameterizedCasesTest extends InitializedNullHandlingTest
  {
    private static final int NUM_THREADS = 1;
    private static final int FRAME_SIZE = 1_000_000;
    private static final int SMALL_FRAME_SIZE = 1_000;
    private static final RowSignature LONG_SIGNATURE = RowSignature.builder().add("x", ColumnType.LONG).build();
    private static final List<KeyColumn> LONG_SORT_KEY = ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING));
    private static final RowSignature LONG_SORTABLE_SIGNATURE =
        FrameWriters.sortableSignature(LONG_SIGNATURE, LONG_SORT_KEY);
    private static final RowSignature STRING_SIGNATURE = RowSignature.builder().add("s", ColumnType.STRING).build();
    private static final List<KeyColumn> STRING_SORT_KEY = ImmutableList.of(new KeyColumn("s", KeyOrder.ASCENDING));
    private static final RowSignature STRING_SORTABLE_SIGNATURE =
        FrameWriters.sortableSignature(STRING_SIGNATURE, STRING_SORT_KEY);

    @RegisterExtension
    public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

    private FrameProcessorExecutor exec;

    @BeforeEach
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(NUM_THREADS, "super-sorter-test-%d"))
      );
    }

    @AfterEach
    public void tearDown()
    {
      exec.getExecutorService().shutdownNow();
    }

    private static class ListenerDelayingFrameProcessorExecutor extends FrameProcessorExecutor
    {
      private int asExecutorCalls;
      private Runnable pendingListener;

      private ListenerDelayingFrameProcessorExecutor()
      {
        super(MoreExecutors.listeningDecorator(Execs.multiThreaded(NUM_THREADS, "super-sorter-test-%d")));
      }

      @Override
      public Executor asExecutor(final String cancellationId)
      {
        // The worker callback is registered first; delay the output-partitions listener registered by run().
        if (++asExecutorCalls == 2) {
          return command -> pendingListener = command;
        }
        return super.asExecutor(cancellationId);
      }

      private void runListener()
      {
        pendingListener.run();
      }
    }

    @Test
    public void testSingleEmptyInputChannel_fileStorage() throws Exception
    {
      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.writable().close();

      final SettableFuture<ClusterByPartitions> outputPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final File tempFolder = temporaryFolder.newFolder();
      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel.readable()),
          FrameReader.create(RowSignature.empty()),
          Collections.emptyList(),
          outputPartitionsFuture,
          exec,
          FrameProcessorDecorator.NONE,
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          FrameType.latestRowBased(),
          2,
          2,
          FRAME_SIZE,
          SuperSorter.UNLIMITED,
          null,
          superSorterProgressTracker,
          false,
          null
      );

      superSorter.setNoWorkRunnable(() -> outputPartitionsFuture.set(ClusterByPartitions.oneUniversalPartition()));
      final OutputChannels channels = superSorter.run().get();
      Assertions.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assertions.assertTrue(channel.isFinished());
      Assertions.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.close();
    }

    @Test
    public void testSingleEmptyInputChannel_immediately_fileStorage() throws Exception
    {
      exec.getExecutorService().shutdownNow();
      final ListenerDelayingFrameProcessorExecutor listenerDelayingExec =
          new ListenerDelayingFrameProcessorExecutor();
      exec = listenerDelayingExec;

      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.writable().close();

      final SuperSorterProgressTracker superSorterProgressTracker =
          Mockito.spy(new SuperSorterProgressTracker());

      final File tempFolder = temporaryFolder.newFolder();
      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel.readable()),
          FrameReader.create(RowSignature.empty()),
          Collections.emptyList(),
          Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
          exec,
          FrameProcessorDecorator.NONE,
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          FrameType.latestRowBased(),
          2,
          2,
          FRAME_SIZE,
          -1,
          null,
          superSorterProgressTracker,
          false,
          null
      );

      final OutputChannels channels = superSorter.run().get();
      Assertions.assertEquals(1, channels.getAllChannels().size());
      Mockito.verify(superSorterProgressTracker).setTotalMergersForUltimateLevel(1L);
      Assertions.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);

      listenerDelayingExec.runListener();

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assertions.assertTrue(channel.isFinished());
      channel.close();
    }

    @Test
    public void testLimitHint() throws Exception
    {
      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.writable().close();

      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final File tempFolder = temporaryFolder.newFolder();
      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel.readable()),
          FrameReader.create(RowSignature.empty()),
          Collections.emptyList(),
          Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
          exec,
          FrameProcessorDecorator.NONE,
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          FrameType.latestRowBased(),
          2,
          2,
          FRAME_SIZE,
          3,
          null,
          superSorterProgressTracker,
          false,
          null
      );

      final OutputChannels channels = superSorter.run().get();
      Assertions.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assertions.assertTrue(channel.isFinished());
      Assertions.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.close();
    }

    @Test
    public void testLimitHint_inputExceedsBuffer_sortsInMemory() throws Exception
    {
      // maxActiveProcessors = 1 and maxChannelsPerMerger = 4 give an input buffer of 4 frames, smaller than the 10
      // input frames. The 3-row limit fits in one small frame, so truncating merges keep the sort in memory. Without
      // truncation, the retained run would outgrow its one-frame cap and spill.
      final SortResult result = sortLongs(
          makeLongChannels(shuffledLongs(100), 10, 1),
          ClusterByPartitions.oneUniversalPartition(),
          false,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          3
      );

      Assertions.assertEquals(List.of(List.of(0L, 1L, 2L)), result.partitions());
      Assertions.assertEquals(0, result.intermediateChannelsOpened(), "low-limit sort must not spill to disk");
      MatcherAssert.assertThat(result.truncatingMerges(), Matchers.greaterThan(1));
    }

    @Test
    public void testLimitHint_limitTooLargeForMemory_spills() throws Exception
    {
      // The 95-row limit needs more than its cap of one small frame (a quarter of the 4-frame buffer), so the sorter
      // skips in-memory top-K mode and spills.
      final SortResult result = sortLongs(
          makeLongChannels(shuffledLongs(100), 10, 1),
          ClusterByPartitions.oneUniversalPartition(),
          false,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          95
      );

      Assertions.assertEquals(List.of(longRange(0, 95)), result.partitions());
      MatcherAssert.assertThat(result.intermediateChannelsOpened(), Matchers.greaterThan(0));
      Assertions.assertEquals(0, result.truncatingMerges(), "estimate must skip truncating merges up front");
    }

    @Test
    public void testLimitHint_truncatingMergeOverflows_spills() throws Exception
    {
      // 20 frames of 10 rows, read in order. Short rows come first, so the estimate passes and truncating merges keep
      // a retained run holding "a00" and "a01". Frame 6 brings 3 long rows that also sort into the top 5. They need two
      // small frames, so that truncating merge overflows. The retained run and the merge's input go back into the
      // buffer, and enough input remains that the sorter finishes in external mode, accounting for the frames dropped
      // by earlier truncating merges.
      final String padding = "x".repeat(400);
      final List<String> values = new ArrayList<>();
      values.add("a00");
      values.add("a01");
      for (int i = 0; values.size() < 50; i++) {
        values.add(StringUtils.format("b%02d", i));
      }
      for (int i = 1; i <= 3; i++) {
        values.add("a" + i + padding);
      }
      for (int i = 50; values.size() < 200; i++) {
        values.add(StringUtils.format("b%02d", i));
      }

      final SortResult result = sortRows(
          makeStringChannels(values, 10),
          STRING_SORTABLE_SIGNATURE,
          STRING_SORT_KEY,
          ClusterByPartitions.oneUniversalPartition(),
          false,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          5
      );

      Assertions.assertEquals(
          List.of(List.of("a00", "a01", "a1" + padding, "a2" + padding, "a3" + padding)),
          result.partitions()
      );
      MatcherAssert.assertThat(result.intermediateChannelsOpened(), Matchers.greaterThan(0));
      MatcherAssert.assertThat(
          "at least one truncating merge succeeds before the one that overflows",
          result.truncatingMerges(),
          Matchers.greaterThanOrEqualTo(2)
      );
    }

    @Test
    public void testLimitHint_tiesAtLimit_sortsInMemory() throws Exception
    {
      final List<Long> values = new ArrayList<>();
      for (final long value : shuffledLongs(100)) {
        values.add(value % 10);
      }

      final SortResult result = sortLongs(
          makeLongChannels(values, 10, 1),
          ClusterByPartitions.oneUniversalPartition(),
          false,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          15
      );

      final List<Long> expected = new ArrayList<>(Collections.nCopies(10, 0L));
      expected.addAll(Collections.nCopies(5, 1L));
      Assertions.assertEquals(List.of(expected), result.partitions());
      Assertions.assertEquals(0, result.intermediateChannelsOpened());
      MatcherAssert.assertThat(result.truncatingMerges(), Matchers.greaterThan(1));
    }

    @Test
    public void testLimitHint_twoPartitions_limitAppliesAcrossPartitions() throws Exception
    {
      // Partitions are [null, 2) and [2, null), so the 3-row limit takes two rows from the first, one from the second.
      final RowKey boundary = KeyTestUtils.createKey(
          KeyTestUtils.createKeySignature(LONG_SORT_KEY, LONG_SORTABLE_SIGNATURE),
          FrameType.latestRowBased(),
          2L
      );
      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(new ClusterByPartition(null, boundary), new ClusterByPartition(boundary, null))
      );

      final SortResult result = sortLongs(
          makeLongChannels(shuffledLongs(100), 10, 1),
          partitions,
          false,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          3
      );

      Assertions.assertEquals(List.of(List.of(0L, 1L), List.of(2L)), result.partitions());
      Assertions.assertEquals(0, result.intermediateChannelsOpened());
      MatcherAssert.assertThat(result.truncatingMerges(), Matchers.greaterThan(1));
    }

    @Test
    public void testLimitHint_partitionsDeferred_sortsInMemory() throws Exception
    {
      final SortResult result = sortLongs(
          makeLongChannels(shuffledLongs(100), 10, 1),
          ClusterByPartitions.oneUniversalPartition(),
          true,
          exec,
          1,
          4,
          SMALL_FRAME_SIZE,
          3
      );

      Assertions.assertEquals(List.of(List.of(0L, 1L, 2L)), result.partitions());
      Assertions.assertEquals(0, result.intermediateChannelsOpened());
      MatcherAssert.assertThat(result.truncatingMerges(), Matchers.greaterThan(1));
    }

    @Test
    public void testLimitHint_concurrentProcessors_sortsInMemory() throws Exception
    {
      final FrameProcessorExecutor multiThreadedExec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(4, "super-sorter-test-concurrent-%d"))
      );

      try {
        for (int i = 0; i < 20; i++) {
          // 2 processors, 8-frame buffer, 4 input channels on a multithreaded executor, so batcher and truncating merge
          // callbacks complete on different threads. Repeated to catch races in the handoff between them.
          final SortResult result = sortLongs(
              makeLongChannels(shuffledLongs(1000), 10, 4),
              ClusterByPartitions.oneUniversalPartition(),
              false,
              multiThreadedExec,
              2,
              4,
              SMALL_FRAME_SIZE,
              7
          );

          Assertions.assertEquals(List.of(longRange(0, 7)), result.partitions());
          Assertions.assertEquals(0, result.intermediateChannelsOpened());
          MatcherAssert.assertThat(result.truncatingMerges(), Matchers.greaterThan(1));
        }
      }
      finally {
        multiThreadedExec.getExecutorService().shutdownNow();
      }
    }

    /**
     * Output values of a {@link #sortRows} call, one list per partition, the number of intermediate (spill) channels
     * the sort opened, and the number of truncating merges it launched, including any that overflowed.
     */
    private record SortResult(List<List<Object>> partitions, int intermediateChannelsOpened, int truncatingMerges)
    {
    }

    /**
     * Sorts the single-column {@link #LONG_SIGNATURE} rows in {@code inputChannels} by {@link #LONG_SORT_KEY}.
     */
    private SortResult sortLongs(
        final List<ReadableFrameChannel> inputChannels,
        final ClusterByPartitions partitions,
        final boolean deferPartitions,
        final FrameProcessorExecutor executor,
        final int maxActiveProcessors,
        final int maxChannelsPerMerger,
        final int frameSize,
        final long rowLimit
    ) throws Exception
    {
      return sortRows(
          inputChannels,
          LONG_SORTABLE_SIGNATURE,
          LONG_SORT_KEY,
          partitions,
          deferPartitions,
          executor,
          maxActiveProcessors,
          maxChannelsPerMerger,
          frameSize,
          rowLimit
      );
    }

    /**
     * Sorts the single-column rows in {@code inputChannels} by {@code sortKey}. If {@code deferPartitions}, the sorter
     * learns {@code partitions} only once it has no other work.
     */
    private SortResult sortRows(
        final List<ReadableFrameChannel> inputChannels,
        final RowSignature sortableSignature,
        final List<KeyColumn> sortKey,
        final ClusterByPartitions partitions,
        final boolean deferPartitions,
        final FrameProcessorExecutor executor,
        final int maxActiveProcessors,
        final int maxChannelsPerMerger,
        final int frameSize,
        final long rowLimit
    ) throws Exception
    {
      final SettableFuture<ClusterByPartitions> partitionsFuture = SettableFuture.create();
      if (!deferPartitions) {
        partitionsFuture.set(partitions);
      }

      final AtomicInteger intermediateChannelsOpened = new AtomicInteger();
      final OutputChannelFactory intermediateOutputChannelFactory = new FileOutputChannelFactory(
          temporaryFolder.newFolder(),
          FRAME_SIZE,
          null,
          FrameTestUtil.WT_CONTEXT_LEGACY
      )
      {
        @Override
        public PartitionedOutputChannel openPartitionedChannel(final String name, final boolean deleteAfterRead)
            throws IOException
        {
          intermediateChannelsOpened.incrementAndGet();
          return super.openPartitionedChannel(name, deleteAfterRead);
        }
      };

      // Truncating merges are the only workers that write to a ListWritableFrameChannel.
      final AtomicInteger truncatingMerges = new AtomicInteger();
      final FrameProcessorDecorator truncatingMergeCounter = new FrameProcessorDecorator()
      {
        @Override
        public <T> FrameProcessor<T> decorate(final FrameProcessor<T> processor)
        {
          if (processor.outputChannels().stream().anyMatch(channel -> channel instanceof ListWritableFrameChannel)) {
            truncatingMerges.incrementAndGet();
          }
          return processor;
        }
      };

      final FrameReader frameReader = FrameReader.create(sortableSignature);
      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          sortKey,
          partitionsFuture,
          executor,
          truncatingMergeCounter,
          new FileOutputChannelFactory(temporaryFolder.newFolder(), FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          intermediateOutputChannelFactory,
          FrameType.latestRowBased(),
          maxActiveProcessors,
          maxChannelsPerMerger,
          frameSize,
          rowLimit,
          null,
          new SuperSorterProgressTracker(),
          false,
          null
      );

      if (deferPartitions) {
        superSorter.setNoWorkRunnable(() -> partitionsFuture.set(partitions));
      }

      final List<List<Object>> outputRows = new ArrayList<>();
      for (final OutputChannel channel : superSorter.run().get(1, TimeUnit.MINUTES).getAllChannels()) {
        final List<Object> rows = new ArrayList<>();
        FrameTestUtil.readRowsFromFrameChannel(channel.getReadableChannel(), frameReader)
                     .forEach(row -> rows.add(row.get(0)));
        outputRows.add(rows);
      }

      return new SortResult(outputRows, intermediateChannelsOpened.get(), truncatingMerges.get());
    }

    /**
     * Writes {@code values} as {@link #LONG_SIGNATURE} frames of {@code rowsPerFrame} rows each, each frame sorted
     * within itself, distributed round-robin across {@code numChannels} channels.
     */
    private static List<ReadableFrameChannel> makeLongChannels(
        final List<Long> values,
        final int rowsPerFrame,
        final int numChannels
    ) throws IOException
    {
      final List<Row> rows = new ArrayList<>();
      for (final long value : values) {
        rows.add(new MapBasedRow(0L, Map.of("x", value)));
      }

      final RowBasedCursorFactory<Row> cursorFactory =
          new RowBasedCursorFactory<>(Sequences.simple(rows), RowAdapters.standardRow(), LONG_SIGNATURE);

      return makeRoundRobinChannels(
          FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                              .maxRowsPerFrame(rowsPerFrame)
                              .sortBy(LONG_SORT_KEY)
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(FRAME_SIZE)))
                              .frameType(FrameType.latestRowBased())
                              .frames(),
          numChannels
      );
    }

    /**
     * Writes {@code values} in order as {@link #STRING_SIGNATURE} frames of {@code rowsPerFrame} rows each, each frame
     * sorted within itself, in a single channel.
     */
    private static List<ReadableFrameChannel> makeStringChannels(final List<String> values, final int rowsPerFrame)
        throws IOException
    {
      final List<Row> rows = new ArrayList<>();
      for (final String value : values) {
        rows.add(new MapBasedRow(0L, Map.of("s", value)));
      }

      final RowBasedCursorFactory<Row> cursorFactory =
          new RowBasedCursorFactory<>(Sequences.simple(rows), RowAdapters.standardRow(), STRING_SIGNATURE);

      return makeRoundRobinChannels(
          FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                              .maxRowsPerFrame(rowsPerFrame)
                              .sortBy(STRING_SORT_KEY)
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(FRAME_SIZE)))
                              .frameType(FrameType.latestRowBased())
                              .frames(),
          1
      );
    }

    /**
     * Returns the values {@code [0, n)} in a fixed shuffled order. Requires {@code n} coprime with 37.
     */
    private static List<Long> shuffledLongs(final int n)
    {
      final List<Long> values = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        values.add((long) i * 37 % n);
      }
      return values;
    }

    private static List<Long> longRange(final long start, final long end)
    {
      return LongStream.range(start, end).boxed().toList();
    }
  }

  /**
   * Parameterized test cases that use {@link TestIndex#getNoRollupIncrementalTestIndex} with various frame sizes,
   * numbers of channels, and worker configurations.
   */
  public static class ParameterizedCasesTest extends InitializedNullHandlingTest
  {
    private static CursorFactory CURSOR_FACTORY;
    private static RowSignature CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER;

    /**
     * Static cache of sorted versions of the {@link #CURSOR_FACTORY} dataset, to speed up tests.
     * Cleared in {@link #tearDownClass()}.
     */
    private static final Map<ClusterBy, List<List<Object>>> SORTED_TEST_ROWS = new HashMap<>();

    @RegisterExtension
    public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

    private RowSignature signature;
    private FrameProcessorExecutor exec;
    private List<ReadableFrameChannel> inputChannels;
    private FrameReader frameReader;

    /**
     * One case of the {@link #constructorFeeder()} matrix. Supplied as a single test-method parameter rather than
     * as ten positional ones, so the test signatures stay readable.
     */
    record SortCase(
        FrameType outputFrameType,
        int maxRowsPerFrame,
        int maxBytesPerFrame,
        int numChannels,
        int maxActiveProcessors,
        int maxChannelsPerProcessor,
        int numThreads,
        boolean isComposedStorage,
        boolean partitionsDeferred,
        long limitHint
    )
    {
    }

    /**
     * Cases for the tests below. These are {@link ParameterizedTest} rather than a parameterized class
     * for performance reasons: <a href="https://github.com/apache/maven-surefire/issues/3439">maven-surefire#3439</a>.
     */
    public static Iterable<SortCase> constructorFeeder()
    {
      final List<SortCase> constructors = new ArrayList<>();

      final FrameType[] rowBasedFrameTypes =
          Arrays.stream(FrameType.values()).filter(FrameType::isRowBased).toArray(FrameType[]::new);

      // Add some constructors for testing maxRowsPerFrame > 1. Later on, we'll add some for maxRowsPerFrame = 1.
      for (FrameType outputFrameType : rowBasedFrameTypes) {
        for (int maxRowsPerFrame : new int[]{Integer.MAX_VALUE, 50}) {
          for (int maxBytesPerFrame : new int[]{20_000, 2_000_000}) {
            for (int numChannels : new int[]{1, 3}) {
              for (int maxActiveProcessors : new int[]{1, 3}) {
                for (int maxChannelsPerProcessor : new int[]{2, 7}) {
                  for (int numThreads : new int[]{1, 3}) {
                    for (boolean isComposedStorage : new boolean[]{true, false}) {
                      for (boolean partitionsDeferred : new boolean[]{true, false}) {
                        for (long limitHint : new long[]{SuperSorter.UNLIMITED, 3, 1_000}) {
                          constructors.add(
                              new SortCase(
                                  outputFrameType,
                                  maxRowsPerFrame,
                                  maxBytesPerFrame,
                                  numChannels,
                                  maxActiveProcessors,
                                  maxChannelsPerProcessor,
                                  numThreads,
                                  isComposedStorage,
                                  partitionsDeferred,
                                  limitHint
                              )
                          );
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }

      // Add some constructors for testing maxRowsPerFrame = 1. This isn't part of the full matrix since it's quite
      // slow, but we still want to exercise it a bit.
      for (boolean isComposedStorage : new boolean[]{true, false}) {
        for (long limitHint : new long[]{SuperSorter.UNLIMITED, 3, 1_000}) {
          constructors.add(
              new SortCase(
                  FrameType.latestRowBased(),
                  1 /* maxRowsPerFrame */,
                  20_000 /* maxBytesPerFrame */,
                  3 /* numChannels */,
                  2 /* maxActiveProcessors */,
                  3 /* maxChannelsPerProcessor */,
                  1 /* numThreads */,
                  isComposedStorage,
                  false /* partitionsDeferred */,
                  limitHint
              )
          );
        }
      }

      return constructors;
    }

    @BeforeAll
    public static void setUpClass()
    {
      CURSOR_FACTORY = new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
      CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER =
          FrameSequenceBuilder.signatureWithRowNumber(CURSOR_FACTORY.getRowSignature());
    }

    @AfterAll
    public static void tearDownClass()
    {
      CURSOR_FACTORY = null;
      CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER = null;
      SORTED_TEST_ROWS.clear();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
      if (exec != null) {
        exec.getExecutorService().shutdownNow();
        if (!exec.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("Executor did not terminate after 5 seconds");
        }
      }
    }

    /**
     * Creates input channels.
     *
     * Sets {@link #inputChannels}, {@link #signature}, and {@link #frameReader}.
     */
    private void setUpInputChannels(final SortCase sortCase, final ClusterBy clusterBy) throws Exception
    {
      if (signature != null || inputChannels != null) {
        throw new ISE("Channels already created for this case");
      }

      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(
              Execs.multiThreaded(sortCase.numThreads(), getClass().getSimpleName() + "[%d]")
          )
      );

      final FrameSequenceBuilder frameSequenceBuilder =
          FrameSequenceBuilder.fromCursorFactory(CURSOR_FACTORY)
                              .maxRowsPerFrame(sortCase.maxRowsPerFrame())
                              .sortBy(clusterBy.getColumns())
                              .allocator(
                                  ArenaMemoryAllocator.create(ByteBuffer.allocate(sortCase.maxBytesPerFrame()))
                              )
                              .frameType(FrameType.latestRowBased())
                              .populateRowNumber();

      inputChannels = makeRoundRobinChannels(frameSequenceBuilder.frames(), sortCase.numChannels());
      signature = FrameWriters.sortableSignature(CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER, clusterBy.getColumns());
      frameReader = FrameReader.create(signature);
    }

    private void verifySuperSorter(
        final SortCase sortCase,
        final ClusterBy clusterBy,
        final ClusterByPartitions clusterByPartitions
    ) throws Exception
    {
      final File tempFolder = temporaryFolder.newFolder();
      final OutputChannelFactory outputChannelFactory = sortCase.isComposedStorage() ? new ComposingOutputChannelFactory(
          ImmutableList.of(
              new FileOutputChannelFactory(new File(tempFolder, "1"), sortCase.maxBytesPerFrame(), null, FrameTestUtil.WT_CONTEXT_LEGACY),
              new FileOutputChannelFactory(new File(tempFolder, "2"), sortCase.maxBytesPerFrame(), null, FrameTestUtil.WT_CONTEXT_LEGACY)
          ),
          sortCase.maxBytesPerFrame()
      ) : new FileOutputChannelFactory(tempFolder, sortCase.maxBytesPerFrame(), null, FrameTestUtil.WT_CONTEXT_LEGACY);
      final RowKeyReader keyReader = clusterBy.keyReader(signature, sortCase.outputFrameType());
      final Comparator<RowKey> keyComparator = clusterBy.keyComparator(signature);
      final SettableFuture<ClusterByPartitions> clusterByPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      if (!sortCase.partitionsDeferred()) {
        clusterByPartitionsFuture.set(clusterByPartitions);
      }

      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          clusterBy.getColumns(),
          clusterByPartitionsFuture,
          exec,
          FrameProcessorDecorator.NONE,
          makeOutputChannelFactory(new FileOutputChannelFactory(tempFolder, sortCase.maxBytesPerFrame(), null, FrameTestUtil.WT_CONTEXT_LEGACY)),
          makeOutputChannelFactory(outputChannelFactory),
          sortCase.outputFrameType(),
          sortCase.maxActiveProcessors(),
          sortCase.maxChannelsPerProcessor(),
          sortCase.maxBytesPerFrame(),
          sortCase.limitHint(),
          null,
          superSorterProgressTracker,
          false,
          null
      );

      if (sortCase.partitionsDeferred()) {
        superSorter.setNoWorkRunnable(() -> clusterByPartitionsFuture.set(clusterByPartitions));
      }

      final OutputChannels outputChannels = superSorter.run().get();
      Assertions.assertEquals(clusterByPartitions.size(), outputChannels.getAllChannels().size());
      Assertions.assertEquals(Double.valueOf(1.0), superSorterProgressTracker.snapshot().getProgressDigest());

      final int[] clusterByColumns = clusterBy.getColumns().stream().mapToInt(
          part -> signature.indexOf(part.columnName())
      ).toArray();

      final List<List<Object>> readRows = new ArrayList<>();
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final ClusterByPartition partition = clusterByPartitions.get(partitionNumber);
        final ReadableFrameChannel outputChannel =
            Iterables.getOnlyElement(outputChannels.getChannelsForPartition(partitionNumber)).getReadableChannel();

        // Validate that everything in this channel is in the correct key range.
        FrameTestUtil.readRowsFromFrameChannel(
            outputChannel,
            frameReader
        ).forEach(
            row -> {
              final Object[] array = new Object[clusterByColumns.length];

              for (int i = 0; i < array.length; i++) {
                array[i] = row.get(clusterByColumns[i]);
              }

              final RowKey key = createKey(sortCase, clusterBy, array);

              if (!(partition.getStart() == null || keyComparator.compare(key, partition.getStart()) >= 0)) {
                // Defer formatting of error message until it's actually needed
                Assertions.fail(
                    StringUtils.format(
                        "Key %s >= partition %,d start %s",
                        keyReader.read(key),
                        partitionNumber,
                        partition.getStart() == null ? null : keyReader.read(partition.getStart())
                    )
                );
              }

              if (!(partition.getEnd() == null || keyComparator.compare(key, partition.getEnd()) < 0)) {
                Assertions.fail(
                    StringUtils.format(
                        "Key %s < partition %,d end %s",
                        keyReader.read(key),
                        partitionNumber,
                        partition.getEnd() == null ? null : keyReader.read(partition.getEnd())
                    )
                );
              }

              readRows.add(row);
            }
        );
      }

      if (sortCase.limitHint() != SuperSorter.UNLIMITED) {
        MatcherAssert.assertThat(readRows.size(), Matchers.greaterThanOrEqualTo(Ints.checkedCast(sortCase.limitHint())));
      }

      final Sequence<List<Object>> expectedRows =
          Sequences.simple(getOrComputeSortedTestRows(clusterBy))
                   .limit(sortCase.limitHint() == SuperSorter.UNLIMITED ? Long.MAX_VALUE : readRows.size());

      FrameTestUtil.assertRowsEqual(expectedRows, Sequences.simple(readRows));
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByQualityLongAscRowNumberAsc_onePartition(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);
      verifySuperSorter(sortCase, clusterBy, ClusterByPartitions.oneUniversalPartition());
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByQualityLongAscRowNumberAsc_twoPartitionsOneEmpty(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final RowKey zeroZero = createKey(sortCase, clusterBy, 0L, 0L);
      verifySuperSorter(
          sortCase,
          clusterBy,
          new ClusterByPartitions(
              ImmutableList.of(
                  new ClusterByPartition(null, zeroZero), // empty partition
                  new ClusterByPartition(zeroZero, null) // all data goes in here
              )
          )
      );
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByQualityDescRowNumberAsc_fourPartitions(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("quality", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, "travel", 8L),
                  createKey(sortCase, clusterBy, "premium", 506L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, "premium", 506L),
                  createKey(sortCase, clusterBy, "mezzanine", 204L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, "mezzanine", 204L),
                  createKey(sortCase, clusterBy, "health", 900L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, "health", 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(sortCase, clusterBy, partitions);
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByTimeAscMarketAscRowNumberAsc_fourPartitions(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn(ColumnHolder.TIME_COLUMN_NAME, KeyOrder.ASCENDING),
              new KeyColumn("market", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1294790400000L, "spot", 0L),
                  createKey(sortCase, clusterBy, 1296864000000L, "spot", 302L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1296864000000L, "spot", 302L),
                  createKey(sortCase, clusterBy, 1298851200000L, "spot", 604L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1298851200000L, "spot", 604L),
                  createKey(sortCase, clusterBy, 1300838400000L, "total_market", 906L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1300838400000L, "total_market", 906L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(sortCase, clusterBy, partitions);
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByPlacementishDescRowNumberAsc_fourPartitions(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("placementish", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, ImmutableList.of("preferred", "t"), 7L),
                  createKey(sortCase, clusterBy, ImmutableList.of("p", "preferred"), 506L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, ImmutableList.of("p", "preferred"), 506L),
                  createKey(sortCase, clusterBy, ImmutableList.of("m", "preferred"), 204L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, ImmutableList.of("m", "preferred"), 204L),
                  createKey(sortCase, clusterBy, ImmutableList.of("h", "preferred"), 900L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, ImmutableList.of("h", "preferred"), 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(sortCase, clusterBy, partitions);
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1800L, 8L),
                  createKey(sortCase, clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1600L, 506L),
                  createKey(sortCase, clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1400L, 204L),
                  createKey(sortCase, clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(sortCase, clusterBy, partitions);
    }

    @ParameterizedTest
    @MethodSource("constructorFeeder")
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions_durableStorage(final SortCase sortCase) throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(sortCase, clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1800L, 8L),
                  createKey(sortCase, clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1600L, 506L),
                  createKey(sortCase, clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1400L, 204L),
                  createKey(sortCase, clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(sortCase, clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(sortCase, clusterBy, partitions);
    }

    private RowKey createKey(final SortCase sortCase, final ClusterBy clusterBy, final Object... objects)
    {
      final RowSignature keySignature = KeyTestUtils.createKeySignature(clusterBy.getColumns(), signature);
      return KeyTestUtils.createKey(keySignature, sortCase.outputFrameType(), objects);
    }

    /**
     * Retrieve sorted test rows from {@link #SORTED_TEST_ROWS}, or else compute using
     * {@link #computeSortedTestRows(ClusterBy)}.
     */
    private static List<List<Object>> getOrComputeSortedTestRows(final ClusterBy clusterBy)
    {
      return SORTED_TEST_ROWS.computeIfAbsent(clusterBy, SuperSorterTest.ParameterizedCasesTest::computeSortedTestRows);
    }

    /**
     * Sort test rows from {@link TestIndex#getNoRollupMMappedTestIndex()} by the given {@link ClusterBy}.
     */
    private static List<List<Object>> computeSortedTestRows(final ClusterBy clusterBy)
    {
      final QueryableIndexCursorFactory cursorFactory =
          new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
      final RowSignature signature =
          FrameWriters.sortableSignature(
              FrameSequenceBuilder.signatureWithRowNumber(cursorFactory.getRowSignature()),
              clusterBy.getColumns()
          );
      final RowSignature keySignature = KeyTestUtils.createKeySignature(clusterBy.getColumns(), signature);
      final int[] clusterByColumns =
          clusterBy.getColumns().stream().mapToInt(part -> signature.indexOf(part.columnName())).toArray();
      final Comparator<RowKey> keyComparator = clusterBy.keyComparator(keySignature);

      return Sequences.sort(
          FrameTestUtil.readRowsFromCursorFactory(cursorFactory, signature, true),
          Comparator.comparing(
              row -> {
                final Object[] array = new Object[clusterByColumns.length];

                for (int i = 0; i < array.length; i++) {
                  array[i] = row.get(clusterByColumns[i]);
                }

                return KeyTestUtils.createKey(keySignature, FrameType.latestRowBased(), array);
              },
              keyComparator
          )
      ).toList();
    }
  }

  /**
   * Parameterized test cases for the combiner functionality.
   */
  @ParameterizedClass
  @MethodSource("constructorFeeder")
  public static class CombinerTest extends InitializedNullHandlingTest
  {
    private static final int FRAME_SIZE = 1_000_000;

    private static final RowSignature SIGNATURE = RowSignature.builder()
                                                              .add("key", ColumnType.STRING)
                                                              .add("value", ColumnType.LONG)
                                                              .build();

    private static final ClusterBy CLUSTER_BY = new ClusterBy(
        ImmutableList.of(new KeyColumn("key", KeyOrder.ASCENDING)),
        0
    );

    private static final RowSignature SORTABLE_SIGNATURE =
        FrameWriters.sortableSignature(SIGNATURE, CLUSTER_BY.getColumns());

    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();
      for (int maxRowsPerFrame : new int[]{1, 2, 3, 4, 5, 6}) {
        for (int maxChannelsPerMerger : new int[]{2, 3}) {
          constructors.add(new Object[]{maxRowsPerFrame, maxChannelsPerMerger});
        }
      }
      return constructors;
    }

    @RegisterExtension
    public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

    private final int maxRowsPerFrame;
    private final int maxChannelsPerMerger;
    private FrameProcessorExecutor exec;

    public CombinerTest(final int maxRowsPerFrame, final int maxChannelsPerMerger)
    {
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.maxChannelsPerMerger = maxChannelsPerMerger;
    }

    @BeforeEach
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "super-sorter-combiner-test-%d"))
      );
    }

    @AfterEach
    public void tearDown()
    {
      exec.getExecutorService().shutdownNow();
    }

    /**
     * Test that combining works in direct mode: input data with duplicate keys produces fewer output rows
     * with combined values.
     */
    @Test
    public void testCombineDirectMode() throws Exception
    {
      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{"a", 1L}, {"b", 2L}, {"c", 3L}},
          new Object[][]{{"a", 10L}, {"b", 20L}, {"c", 30L}}
      );

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          2,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", 11L),
              ImmutableList.of("b", 22L),
              ImmutableList.of("c", 33L)
          ),
          rows
      );
    }

    /**
     * Test that combining works in external mode with many channels forcing multi-level merge.
     */
    @Test
    public void testCombineExternalMode() throws Exception
    {
      final List<Object[][]> channelData = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        channelData.add(new Object[][]{{"a", 1L}, {"b", 1L}});
      }

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          1,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", 10L),
              ImmutableList.of("b", 10L)
          ),
          rows
      );
    }

    /**
     * Test that combining works correctly when output is partitioned.
     */
    @Test
    public void testCombineWithPartitions() throws Exception
    {
      // Two partitions: [null, c) and [c, null)
      final RowKey partitionBoundary = KeyTestUtils.createKey(
          KeyTestUtils.createKeySignature(CLUSTER_BY.getColumns(), SORTABLE_SIGNATURE),
          FrameType.latestRowBased(),
          "c"
      );

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(null, partitionBoundary),
              new ClusterByPartition(partitionBoundary, null)
          )
      );

      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{"a", 1L}, {"b", 2L}, {"c", 3L}, {"d", 4L}},
          new Object[][]{{"a", 10L}, {"b", 20L}, {"c", 30L}, {"d", 40L}}
      );

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          partitions,
          2,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", 11L),
              ImmutableList.of("b", 22L),
              ImmutableList.of("c", 33L),
              ImmutableList.of("d", 44L)
          ),
          rows
      );
    }

    /**
     * Test combining when all rows across all channels have the same key.
     */
    @Test
    public void testCombineAllSameKey() throws Exception
    {
      final List<Object[][]> channelData = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        channelData.add(new Object[][]{{"x", 1L}, {"x", 2L}, {"x", 3L}});
      }

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          2,
          SuperSorter.UNLIMITED
      );

      // 5 channels * (1 + 2 + 3) = 30
      Assertions.assertEquals(
          ImmutableList.of(ImmutableList.of("x", 30L)),
          rows
      );
    }

    /**
     * Test combining with row limits.
     */
    @Test
    public void testCombineWithRowLimit() throws Exception
    {
      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{"a", 1L}, {"b", 2L}, {"c", 3L}},
          new Object[][]{{"a", 10L}, {"b", 20L}, {"c", 30L}}
      );

      for (int limit = 1; limit <= 3; limit++) {
        final List<List<Object>> rows = runCombiningSuperSorter(
            SIGNATURE,
            CLUSTER_BY,
            SORTABLE_SIGNATURE,
            channelData,
            ClusterByPartitions.oneUniversalPartition(),
            2,
            limit
        );

        Assertions.assertEquals(
            limit,
            rows.size(),
            StringUtils.format("limit[%d]: expected exactly %d row(s), got %d", limit, limit, rows.size())
        );
        Assertions.assertEquals(ImmutableList.of("a", 11L), rows.get(0));
        if (limit >= 2) {
          Assertions.assertEquals(ImmutableList.of("b", 22L), rows.get(1));
        }
        if (limit >= 3) {
          Assertions.assertEquals(ImmutableList.of("c", 33L), rows.get(2));
        }
      }
    }

    /**
     * Test combining with a row limit when input exceeds the buffer, so the sorter runs in in-memory top-K mode. Each
     * key's rows arrive across several truncating merges, and every one must be combined into the output.
     */
    @Test
    public void testCombineWithRowLimit_inputExceedsBuffer() throws Exception
    {
      final List<Object[][]> channelData = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        channelData.add(new Object[][]{{"a", 1L}, {"b", 1L}, {"c", 1L}});
      }

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          2,
          2
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", 10L),
              ImmutableList.of("b", 10L)
          ),
          rows
      );
    }

    /**
     * Test combining with rowLimit = 1 and all-same-key input: single combined row.
     */
    @Test
    public void testCombineAllSameKeyWithRowLimit1() throws Exception
    {
      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{"x", 1L}},
          new Object[][]{{"x", 2L}},
          new Object[][]{{"x", 3L}}
      );

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          2,
          1
      );

      // All rows combine to one; rowLimit = 1 is satisfied.
      Assertions.assertEquals(
          ImmutableList.of(ImmutableList.of("x", 6L)),
          rows
      );
    }

    /**
     * Test combining with a single input channel where duplicate keys are within the same sorted stream.
     */
    @Test
    public void testCombineSingleChannel() throws Exception
    {
      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{"a", 1L}, {"a", 2L}, {"b", 3L}, {"b", 4L}}
      );

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          1,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", 3L),
              ImmutableList.of("b", 7L)
          ),
          rows
      );
    }

    /**
     * Test combining with empty input channels.
     */
    @Test
    public void testCombineEmptyInput() throws Exception
    {
      final List<Object[][]> channelData = ImmutableList.of(new Object[][]{});

      final List<List<Object>> rows = runCombiningSuperSorter(
          SIGNATURE,
          CLUSTER_BY,
          SORTABLE_SIGNATURE,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          1,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(ImmutableList.of(), rows);
    }

    /**
     * Test combining with a LONG key column, which exercises {@link TrackingColumnValueSelector}.
     */
    @Test
    public void testCombineWithLongKey() throws Exception
    {
      final RowSignature longKeySignature =
          RowSignature.builder()
                      .add("key", ColumnType.LONG)
                      .add("value", ColumnType.LONG)
                      .build();

      final ClusterBy longKeyClusterBy = new ClusterBy(
          ImmutableList.of(new KeyColumn("key", KeyOrder.ASCENDING)),
          0
      );

      final RowSignature longKeySortableSignature =
          FrameWriters.sortableSignature(longKeySignature, longKeyClusterBy.getColumns());

      final List<Object[][]> channelData = ImmutableList.of(
          new Object[][]{{1L, 10L}, {2L, 20L}, {3L, 30L}},
          new Object[][]{{1L, 100L}, {2L, 200L}, {3L, 300L}}
      );

      final List<List<Object>> rows = runCombiningSuperSorter(
          longKeySignature,
          longKeyClusterBy,
          longKeySortableSignature,
          channelData,
          ClusterByPartitions.oneUniversalPartition(),
          2,
          SuperSorter.UNLIMITED
      );

      Assertions.assertEquals(
          ImmutableList.of(
              ImmutableList.of(1L, 110L),
              ImmutableList.of(2L, 220L),
              ImmutableList.of(3L, 330L)
          ),
          rows
      );
    }

    /**
     * Helper that runs a combining SuperSorter with the given signature, channel data, partitions,
     * maxActiveProcessors, and rowLimit. Returns all output rows across all partitions.
     */
    private List<List<Object>> runCombiningSuperSorter(
        final RowSignature signature,
        final ClusterBy clusterBy,
        final RowSignature sortableSignature,
        final List<Object[][]> channelData,
        final ClusterByPartitions partitions,
        final int maxActiveProcessors,
        final long rowLimit
    ) throws Exception
    {
      final FrameReader frameReader = FrameReader.create(sortableSignature);

      final List<ReadableFrameChannel> channels = new ArrayList<>();
      for (final Object[][] data : channelData) {
        channels.add(makeFrameChannel(signature, clusterBy, data));
      }

      final File tempFolder = temporaryFolder.newFolder();

      final SuperSorter superSorter = new SuperSorter(
          channels,
          frameReader,
          clusterBy.getColumns(),
          Futures.immediateFuture(partitions),
          exec,
          FrameProcessorDecorator.NONE,
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null, FrameTestUtil.WT_CONTEXT_LEGACY),
          FrameType.latestRowBased(),
          maxActiveProcessors,
          maxChannelsPerMerger,
          FRAME_SIZE,
          rowLimit,
          null,
          new SuperSorterProgressTracker(),
          false,
          () -> new SummingFrameCombiner(sortableSignature, 1)
      );

      final OutputChannels outputChannels = superSorter.run().get();

      final List<List<Object>> rows = new ArrayList<>();
      for (final OutputChannel channel : outputChannels.getAllChannels()) {
        FrameTestUtil.readRowsFromFrameChannel(channel.getReadableChannel(), frameReader)
                     .forEach(rows::add);
      }

      return rows;
    }

    private ReadableFrameChannel makeFrameChannel(
        final RowSignature signature,
        final ClusterBy clusterBy,
        final Object[][] rows
    ) throws IOException
    {
      final List<Row> rowList = new ArrayList<>();
      for (final Object[] row : rows) {
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < signature.size(); i++) {
          map.put(signature.getColumnName(i), row[i]);
        }
        rowList.add(new MapBasedRow(0L, map));
      }

      final RowBasedCursorFactory<Row> cursorFactory =
          new RowBasedCursorFactory<>(
              Sequences.simple(rowList),
              RowAdapters.standardRow(),
              signature
          );

      final Sequence<Frame> frames =
          FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .sortBy(clusterBy.getColumns())
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(FRAME_SIZE)))
                              .frameType(FrameType.latestRowBased())
                              .frames();

      final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(100);
      frames.forEach(frame -> {
        try {
          channel.writable().write(frame);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      channel.writable().close();
      return channel.readable();
    }
  }

  /**
   * Distribute frames round-robin to some number of channels.
   */
  private static List<ReadableFrameChannel> makeRoundRobinChannels(
      final Sequence<Frame> frames,
      final int numChannels
  ) throws IOException
  {
    final List<BlockingQueueFrameChannel> channels = new ArrayList<>(numChannels);

    for (int i = 0; i < numChannels; i++) {
      channels.add(new BlockingQueueFrameChannel(2000) /* enough even for 1 row per frame; dataset has < 2000 rows */);
    }

    frames.forEach(
        new Consumer<>()
        {
          private int i;

          @Override
          public void accept(final Frame frame)
          {
            try {
              channels.get(i % channels.size()).writable().write(frame);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            i++;
          }
        }
    );

    final List<ReadableFrameChannel> retVal = new ArrayList<>();

    for (final BlockingQueueFrameChannel channel : channels) {
      channel.writable().close();
      retVal.add(new AlwaysAsyncReadableFrameChannel(channel.readable()));
    }

    return retVal;
  }

  /**
   * Wraps an underlying {@link OutputChannelFactory} in one that uses {@link AlwaysAsyncReadableFrameChannel}
   * for all of its readable channels. This helps catch bugs due to improper usage of {@link ReadableFrameChannel}
   * methods that enable async reads.
   */
  private static OutputChannelFactory makeOutputChannelFactory(final OutputChannelFactory baseFactory)
  {
    return new OutputChannelFactory() {
      @Override
      public OutputChannel openChannel(int partitionNumber) throws IOException
      {
        final OutputChannel channel = baseFactory.openChannel(partitionNumber);
        return OutputChannel.pair(
            channel.getWritableChannel(),
            channel.getFrameMemoryAllocator(),
            () -> new AlwaysAsyncReadableFrameChannel(channel.getReadableChannelSupplier().get()),
            channel.getPartitionNumber()
        );
      }

      @Override
      public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead) throws IOException
      {
        final PartitionedOutputChannel channel = baseFactory.openPartitionedChannel(name, deleteAfterRead);
        return PartitionedOutputChannel.pair(
            channel.getWritableChannel(),
            channel.getFrameMemoryAllocator(),
            () -> new AlwaysAsyncPartitionedReadableFrameChannel(channel.getReadableChannelSupplier().get())
        );
      }

      @Override
      public OutputChannel openNilChannel(int partitionNumber)
      {
        return baseFactory.openNilChannel(partitionNumber);
      }

      @Override
      public boolean isBuffered()
      {
        return baseFactory.isBuffered();
      }
    };
  }
}
