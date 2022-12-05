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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@RunWith(Enclosed.class)
public class SuperSorterTest
{
  private static final Logger log = new Logger(SuperSorterTest.class);

  /**
   * Non-parameterized test cases that
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
    public void testSingleEmptyInputChannel() throws Exception
    {
      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.writable().close();

      final SettableFuture<ClusterByPartitions> outputPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel.readable()),
          FrameReader.create(RowSignature.empty()),
          ClusterBy.none(),
          outputPartitionsFuture,
          exec,
          temporaryFolder.newFolder(),
          new FileOutputChannelFactory(temporaryFolder.newFolder(), FRAME_SIZE),
          () -> ArenaMemoryAllocator.createOnHeap(FRAME_SIZE),
          2,
          2,
          -1,
          null,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> outputPartitionsFuture.set(ClusterByPartitions.oneUniversalPartition()));
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
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final int maxRowsPerFrame;
    private final int maxBytesPerFrame;
    private final int numChannels;
    private final int maxActiveProcessors;
    private final int maxChannelsPerProcessor;
    private final int numThreads;

    private StorageAdapter adapter;
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
        int numThreads
    )
    {
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.maxBytesPerFrame = maxBytesPerFrame;
      this.numChannels = numChannels;
      this.maxActiveProcessors = maxActiveProcessors;
      this.maxChannelsPerProcessor = maxChannelsPerProcessor;
      this.numThreads = numThreads;
    }

    @Parameterized.Parameters(
        name = "maxRowsPerFrame = {0}, "
               + "maxBytesPerFrame = {1}, "
               + "numChannels = {2}, "
               + "maxActiveProcessors = {3}, "
               + "maxChannelsPerProcessor = {4}, "
               + "numThreads = {5}"
    )
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int maxRowsPerFrame : new int[]{Integer.MAX_VALUE, 50, 1}) {
        for (int maxBytesPerFrame : new int[]{20000, 200000}) {
          for (int numChannels : new int[]{1, 3}) {
            for (int maxActiveProcessors : new int[]{1, 2, 4}) {
              for (int maxChannelsPerProcessor : new int[]{2, 3, 8}) {
                for (int numThreads : new int[]{1, 3}) {
                  if (maxActiveProcessors >= maxChannelsPerProcessor) {
                    constructors.add(
                        new Object[]{
                            maxRowsPerFrame,
                            maxBytesPerFrame,
                            numChannels,
                            maxActiveProcessors,
                            maxChannelsPerProcessor,
                            numThreads
                        }
                    );
                  }
                }
              }
            }
          }
        }
      }

      return constructors;
    }

    @Before
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(numThreads, getClass().getSimpleName() + "[%d]"))
      );
      adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
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
          FrameSequenceBuilder.fromAdapter(adapter)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .sortBy(clusterBy.getColumns())
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(maxBytesPerFrame)))
                              .frameType(FrameType.ROW_BASED)
                              .populateRowNumber();

      inputChannels = makeFileChannels(frameSequenceBuilder.frames(), temporaryFolder.newFolder(), numChannels);
      signature = frameSequenceBuilder.signature();
      frameReader = FrameReader.create(signature);
    }

    private OutputChannels verifySuperSorter(
        final ClusterBy clusterBy,
        final ClusterByPartitions clusterByPartitions
    ) throws Exception
    {
      final RowKeyReader keyReader = clusterBy.keyReader(signature);
      final Comparator<RowKey> keyComparator = clusterBy.keyComparator();
      final SettableFuture<ClusterByPartitions> clusterByPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          clusterBy,
          clusterByPartitionsFuture,
          exec,
          temporaryFolder.newFolder(),
          new FileOutputChannelFactory(temporaryFolder.newFolder(), maxBytesPerFrame),
          () -> ArenaMemoryAllocator.createOnHeap(maxBytesPerFrame),
          maxActiveProcessors,
          maxChannelsPerProcessor,
          -1,
          null,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> clusterByPartitionsFuture.set(clusterByPartitions));
      final OutputChannels outputChannels = superSorter.run().get();
      Assert.assertEquals(clusterByPartitions.size(), outputChannels.getAllChannels().size());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);

      final int[] clusterByPartColumns = clusterBy.getColumns().stream().mapToInt(
          part -> signature.indexOf(part.columnName())
      ).toArray();

      final List<Sequence<List<Object>>> outputSequences = new ArrayList<>();
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final ClusterByPartition partition = clusterByPartitions.get(partitionNumber);
        final ReadableFrameChannel outputChannel =
            Iterables.getOnlyElement(outputChannels.getChannelsForPartition(partitionNumber)).getReadableChannel();

        // Validate that everything in this channel is in the correct key range.
        FrameTestUtil.readRowsFromFrameChannel(
            duplicateOutputChannel(outputChannel),
            frameReader
        ).forEach(
            row -> {
              final Object[] array = new Object[clusterByPartColumns.length];

              for (int i = 0; i < array.length; i++) {
                array[i] = row.get(clusterByPartColumns[i]);
              }

              final RowKey key = createKey(clusterBy, array);

              Assert.assertTrue(
                  StringUtils.format(
                      "Key %s >= partition %,d start %s",
                      keyReader.read(key),
                      partitionNumber,
                      partition.getStart() == null ? null : keyReader.read(partition.getStart())
                  ),
                  partition.getStart() == null || keyComparator.compare(key, partition.getStart()) >= 0
              );

              Assert.assertTrue(
                  StringUtils.format(
                      "Key %s < partition %,d end %s",
                      keyReader.read(key),
                      partitionNumber,
                      partition.getEnd() == null ? null : keyReader.read(partition.getEnd())
                  ),
                  partition.getEnd() == null || keyComparator.compare(key, partition.getEnd()) < 0
              );
            }
        );

        outputSequences.add(
            FrameTestUtil.readRowsFromFrameChannel(
                duplicateOutputChannel(outputChannel),
                frameReader
            )
        );
      }

      final Sequence<List<Object>> expectedRows = Sequences.sort(
          FrameTestUtil.readRowsFromAdapter(adapter, signature, true),
          Comparator.comparing(
              row -> {
                final Object[] array = new Object[clusterByPartColumns.length];

                for (int i = 0; i < array.length; i++) {
                  array[i] = row.get(clusterByPartColumns[i]);
                }

                return createKey(clusterBy, array);
              },
              keyComparator
          )
      );

      FrameTestUtil.assertRowsEqual(expectedRows, Sequences.concat(outputSequences));

      return outputChannels;
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_onePartition() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new SortColumn("qualityLong", false),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
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
              new SortColumn("qualityLong", false),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final RowKey zeroZero = createKey(clusterBy, 0L, 0L);
      final OutputChannels outputChannels = verifySuperSorter(
          clusterBy,
          new ClusterByPartitions(
              ImmutableList.of(
                  new ClusterByPartition(null, zeroZero), // empty partition
                  new ClusterByPartition(zeroZero, null) // all data goes in here
              )
          )
      );

      // Verify that one of the partitions is actually empty.
      Assert.assertEquals(
          0,
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(0)).getReadableChannel(),
                  frameReader
              )
          )
      );

      // Verify that the other partition has all data in it.
      Assert.assertEquals(
          adapter.getNumRows(),
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(1)).getReadableChannel(),
                  frameReader
              )
          )
      );
    }

    @Test
    public void test_clusterByQualityDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new SortColumn("quality", true),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
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
              new SortColumn(ColumnHolder.TIME_COLUMN_NAME, false),
              new SortColumn("market", false),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
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
              new SortColumn("placementish", true),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
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
              new SortColumn("qualityLong", true),
              new SortColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
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
  }

  private static List<ReadableFrameChannel> makeFileChannels(
      final Sequence<Frame> frames,
      final File tmpDir,
      final int numChannels
  ) throws IOException
  {
    final List<File> files = new ArrayList<>();
    final List<WritableFrameChannel> writableChannels = new ArrayList<>();

    for (int i = 0; i < numChannels; i++) {
      final File file = new File(tmpDir, StringUtils.format("channel-%d", i));
      files.add(file);
      writableChannels.add(
          new WritableFrameFileChannel(FrameFileWriter.open(Channels.newChannel(new FileOutputStream(file)), null))
      );
    }

    frames.forEach(
        new Consumer<Frame>()
        {
          private int i;

          @Override
          public void accept(final Frame frame)
          {
            try {
              writableChannels.get(i % writableChannels.size()).write(frame);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            i++;
          }
        }
    );

    final List<ReadableFrameChannel> retVal = new ArrayList<>();

    for (int i = 0; i < writableChannels.size(); i++) {
      WritableFrameChannel writableChannel = writableChannels.get(i);
      writableChannel.close();
      retVal.add(new ReadableFileFrameChannel(FrameFile.open(files.get(i))));
    }

    return retVal;
  }

  private static ReadableFrameChannel duplicateOutputChannel(final ReadableFrameChannel channel)
  {
    return new ReadableFileFrameChannel(((ReadableFileFrameChannel) channel).newFrameFileReference());
  }

  private static <T> long countSequence(final Sequence<T> sequence)
  {
    return sequence.accumulate(
        0L,
        (accumulated, in) -> accumulated + 1
    );
  }
}
