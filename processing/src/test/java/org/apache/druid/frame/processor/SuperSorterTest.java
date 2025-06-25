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
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FrameProcessorExecutor exec;

    @Before
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(NUM_THREADS, "super-sorter-test-%d"))
      );
    }

    @After
    public void tearDown()
    {
      exec.getExecutorService().shutdownNow();
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
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          2,
          2,
          SuperSorter.UNLIMITED,
          null,
          superSorterProgressTracker,
          false
      );

      superSorter.setNoWorkRunnable(() -> outputPartitionsFuture.set(ClusterByPartitions.oneUniversalPartition()));
      final OutputChannels channels = superSorter.run().get();
      Assert.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assert.assertTrue(channel.isFinished());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.close();
    }

    @Test
    public void testSingleEmptyInputChannel_immediately_fileStorage() throws Exception
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
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          2,
          2,
          -1,
          null,
          superSorterProgressTracker,
          false
      );

      final OutputChannels channels = superSorter.run().get();
      Assert.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assert.assertTrue(channel.isFinished());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
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
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          2,
          2,
          3,
          null,
          superSorterProgressTracker,
          false
      );

      final OutputChannels channels = superSorter.run().get();
      Assert.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assert.assertTrue(channel.isFinished());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.close();
    }
  }

  /**
   * Parameterized test cases that use {@link TestIndex#getNoRollupIncrementalTestIndex} with various frame sizes,
   * numbers of channels, and worker configurations.
   */
  @RunWith(Parameterized.class)
  public static class ParameterizedCasesTest extends InitializedNullHandlingTest
  {
    private static CursorFactory CURSOR_FACTORY;
    private static RowSignature CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER;

    /**
     * Static cache of sorted versions of the {@link #CURSOR_FACTORY} dataset, to speed up tests.
     * Cleared in {@link #tearDownClass()}.
     */
    private static final Map<ClusterBy, List<List<Object>>> SORTED_TEST_ROWS = new HashMap<>();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final int maxRowsPerFrame;
    private final int maxBytesPerFrame;
    private final int numChannels;
    private final int maxActiveProcessors;
    private final int maxChannelsPerProcessor;
    private final int numThreads;
    private final boolean isComposedStorage;
    private final boolean partitionsDeferred;
    private final long limitHint;

    private RowSignature signature;
    private FrameProcessorExecutor exec;
    private List<ReadableFrameChannel> inputChannels;
    private FrameReader frameReader;

    public ParameterizedCasesTest(
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
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.maxBytesPerFrame = maxBytesPerFrame;
      this.numChannels = numChannels;
      this.maxActiveProcessors = maxActiveProcessors;
      this.maxChannelsPerProcessor = maxChannelsPerProcessor;
      this.numThreads = numThreads;
      this.isComposedStorage = isComposedStorage;
      this.partitionsDeferred = partitionsDeferred;
      this.limitHint = limitHint;
    }

    @Parameterized.Parameters(
        name = "maxRowsPerFrame = {0}, "
               + "maxBytesPerFrame = {1}, "
               + "numChannels = {2}, "
               + "maxActiveProcessors = {3}, "
               + "maxChannelsPerProcessor= {4}, "
               + "numThreads = {5}, "
               + "isComposedStorage = {6}, "
               + "partitionsDeferred = {7}, "
               + "limitHint = {8}"
    )
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      // Add some constructors for testing maxRowsPerFrame > 1. Later on, we'll add some for maxRowsPerFrame = 1.
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
                            new Object[]{
                                maxRowsPerFrame,
                                maxBytesPerFrame,
                                numChannels,
                                maxActiveProcessors,
                                maxChannelsPerProcessor,
                                numThreads,
                                isComposedStorage,
                                partitionsDeferred,
                                limitHint
                            }
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

      // Add some constructors for testing maxRowsPerFrame = 1. This isn't part of the full matrix since it's quite
      // slow, but we still want to exercise it a bit.
      for (boolean isComposedStorage : new boolean[]{true, false}) {
        for (long limitHint : new long[]{SuperSorter.UNLIMITED, 3, 1_000}) {
          constructors.add(
              new Object[]{
                  1 /* maxRowsPerFrame */,
                  20_000 /* maxBytesPerFrame */,
                  3 /* numChannels */,
                  2 /* maxActiveProcessors */,
                  3 /* maxChannelsPerProcessor */,
                  1 /* numThreads */,
                  isComposedStorage,
                  false /* partitionsDeferred */,
                  limitHint
              }
          );
        }
      }

      return constructors;
    }

    @BeforeClass
    public static void setUpClass()
    {
      CURSOR_FACTORY = new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
      CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER =
          FrameSequenceBuilder.signatureWithRowNumber(CURSOR_FACTORY.getRowSignature());
    }

    @AfterClass
    public static void tearDownClass()
    {
      CURSOR_FACTORY = null;
      CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER = null;
      SORTED_TEST_ROWS.clear();
    }

    @Before
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(numThreads, getClass().getSimpleName() + "[%d]"))
      );
    }

    @After
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
    private void setUpInputChannels(final ClusterBy clusterBy) throws Exception
    {
      if (signature != null || inputChannels != null) {
        throw new ISE("Channels already created for this case");
      }

      final FrameSequenceBuilder frameSequenceBuilder =
          FrameSequenceBuilder.fromCursorFactory(CURSOR_FACTORY)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .sortBy(clusterBy.getColumns())
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(maxBytesPerFrame)))
                              .frameType(FrameType.ROW_BASED)
                              .populateRowNumber();

      inputChannels = makeRoundRobinChannels(frameSequenceBuilder.frames(), numChannels);
      signature = FrameWriters.sortableSignature(CURSOR_FACTORY_SIGNATURE_WITH_ROW_NUMBER, clusterBy.getColumns());
      frameReader = FrameReader.create(signature);
    }

    private void verifySuperSorter(
        final ClusterBy clusterBy,
        final ClusterByPartitions clusterByPartitions
    ) throws Exception
    {
      final File tempFolder = temporaryFolder.newFolder();
      final OutputChannelFactory outputChannelFactory = isComposedStorage ? new ComposingOutputChannelFactory(
          ImmutableList.of(
              new FileOutputChannelFactory(new File(tempFolder, "1"), maxBytesPerFrame, null),
              new FileOutputChannelFactory(new File(tempFolder, "2"), maxBytesPerFrame, null)
          ),
          maxBytesPerFrame
      ) : new FileOutputChannelFactory(tempFolder, maxBytesPerFrame, null);
      final RowKeyReader keyReader = clusterBy.keyReader(signature);
      final Comparator<RowKey> keyComparator = clusterBy.keyComparator(signature);
      final SettableFuture<ClusterByPartitions> clusterByPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      if (!partitionsDeferred) {
        clusterByPartitionsFuture.set(clusterByPartitions);
      }

      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          clusterBy.getColumns(),
          clusterByPartitionsFuture,
          exec,
          FrameProcessorDecorator.NONE,
          makeOutputChannelFactory(new FileOutputChannelFactory(tempFolder, maxBytesPerFrame, null)),
          makeOutputChannelFactory(outputChannelFactory),
          maxActiveProcessors,
          maxChannelsPerProcessor,
          limitHint,
          null,
          superSorterProgressTracker,
          false
      );

      if (partitionsDeferred) {
        superSorter.setNoWorkRunnable(() -> clusterByPartitionsFuture.set(clusterByPartitions));
      }

      final OutputChannels outputChannels = superSorter.run().get();
      Assert.assertEquals(clusterByPartitions.size(), outputChannels.getAllChannels().size());
      Assert.assertEquals(Double.valueOf(1.0), superSorterProgressTracker.snapshot().getProgressDigest());

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

              final RowKey key = createKey(clusterBy, array);

              if (!(partition.getStart() == null || keyComparator.compare(key, partition.getStart()) >= 0)) {
                // Defer formatting of error message until it's actually needed
                Assert.fail(
                    StringUtils.format(
                        "Key %s >= partition %,d start %s",
                        keyReader.read(key),
                        partitionNumber,
                        partition.getStart() == null ? null : keyReader.read(partition.getStart())
                    )
                );
              }

              if (!(partition.getEnd() == null || keyComparator.compare(key, partition.getEnd()) < 0)) {
                Assert.fail(
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

      if (limitHint != SuperSorter.UNLIMITED) {
        MatcherAssert.assertThat(readRows.size(), Matchers.greaterThanOrEqualTo(Ints.checkedCast(limitHint)));
      }

      final Sequence<List<Object>> expectedRows =
          Sequences.simple(getOrComputeSortedTestRows(clusterBy))
                   .limit(limitHint == SuperSorter.UNLIMITED ? Long.MAX_VALUE : readRows.size());

      FrameTestUtil.assertRowsEqual(expectedRows, Sequences.simple(readRows));
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_onePartition() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);
      verifySuperSorter(clusterBy, ClusterByPartitions.oneUniversalPartition());
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_twoPartitionsOneEmpty() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final RowKey zeroZero = createKey(clusterBy, 0L, 0L);
      verifySuperSorter(
          clusterBy,
          new ClusterByPartitions(
              ImmutableList.of(
                  new ClusterByPartition(null, zeroZero), // empty partition
                  new ClusterByPartition(zeroZero, null) // all data goes in here
              )
          )
      );
    }

    @Test
    public void test_clusterByQualityDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("quality", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, "travel", 8L),
                  createKey(clusterBy, "premium", 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "premium", 506L),
                  createKey(clusterBy, "mezzanine", 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "mezzanine", 204L),
                  createKey(clusterBy, "health", 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "health", 900L),
                  null
              )
          )
      );

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByTimeAscMarketAscRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn(ColumnHolder.TIME_COLUMN_NAME, KeyOrder.ASCENDING),
              new KeyColumn("market", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1294790400000L, "spot", 0L),
                  createKey(clusterBy, 1296864000000L, "spot", 302L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1296864000000L, "spot", 302L),
                  createKey(clusterBy, 1298851200000L, "spot", 604L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1298851200000L, "spot", 604L),
                  createKey(clusterBy, 1300838400000L, "total_market", 906L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300838400000L, "total_market", 906L),
                  null
              )
          )
      );

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByPlacementishDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("placementish", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("preferred", "t"), 7L),
                  createKey(clusterBy, ImmutableList.of("p", "preferred"), 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("p", "preferred"), 506L),
                  createKey(clusterBy, ImmutableList.of("m", "preferred"), 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("m", "preferred"), 204L),
                  createKey(clusterBy, ImmutableList.of("h", "preferred"), 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("h", "preferred"), 900L),
                  null
              )
          )
      );

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1800L, 8L),
                  createKey(clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1600L, 506L),
                  createKey(clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1400L, 204L),
                  createKey(clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions_durableStorage() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1800L, 8L),
                  createKey(clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1600L, 506L),
                  createKey(clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1400L, 204L),
                  createKey(clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    private RowKey createKey(final ClusterBy clusterBy, final Object... objects)
    {
      final RowSignature keySignature = KeyTestUtils.createKeySignature(clusterBy.getColumns(), signature);
      return KeyTestUtils.createKey(keySignature, objects);
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

                return KeyTestUtils.createKey(keySignature, array);
              },
              keyComparator
          )
      ).toList();
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
