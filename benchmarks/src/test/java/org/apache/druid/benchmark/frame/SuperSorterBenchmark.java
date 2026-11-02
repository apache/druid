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

package org.apache.druid.benchmark.frame;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FileOutputChannelFactory;
import org.apache.druid.frame.processor.FrameProcessorDecorator;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.processor.SuperSorter;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for {@link SuperSorter}, focused on sorts with a row limit whose input is larger than the sorter's
 * in-memory buffer. Input frames are sorted within themselves but not relative to each other, as the sorter expects.
 * Intermediate and final output is written to files under a temporary directory. See {@link Scale} for the input and
 * sorter sizes.
 *
 * After each trial, prints the number of intermediate (spill) channels opened per invocation.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xmx8g"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SuperSorterBenchmark
{
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final int KEY_LENGTH = 20;
  private static final int ROW_LENGTH = 100;
  private static final int NUM_CHANNELS = 8;

  /**
   * Sizes of the input and the sorter's buffer. Rows are about {@link #ROW_LENGTH} bytes each.
   */
  public enum Scale
  {
    /**
     * 1M rows in 100 KB frames, with 16 frames per merger. The buffer is small, so this runs quickly, and limits of
     * 10,000 rows or more do not fit in memory with a single processor.
     */
    SMALL(1_000_000, 100_000, 16),

    /**
     * 5M rows in 1 MB frames, the default frame size, with 200 frames per merger. The buffer is about 200 MB per
     * processor.
     */
    LARGE(5_000_000, 1_000_000, 200);

    private final int numRows;
    private final int frameSize;
    private final int maxChannelsPerMerger;

    Scale(final int numRows, final int frameSize, final int maxChannelsPerMerger)
    {
      this.numRows = numRows;
      this.frameSize = frameSize;
      this.maxChannelsPerMerger = maxChannelsPerMerger;
    }
  }

  @Param({"SMALL", "LARGE"})
  private Scale scale;

  /**
   * {@link SuperSorter#UNLIMITED} is a control: the sort runs the same way with or without in-memory top-K mode.
   */
  @Param({"-1", "100", "10000", "100000"})
  private long rowLimit;

  /**
   * MSQ sorts with a row limit produce a single partition, so they run with one sorter processor. More processors model
   * a multi-partition sort, and multiply the buffer, so the input should still exceed it.
   */
  @Param({"1"})
  private int maxActiveProcessors;

  private final List<KeyColumn> sortKey = ImmutableList.of(new KeyColumn(KEY, KeyOrder.ASCENDING));

  private RowSignature signature;
  private FrameReader frameReader;
  private List<List<Frame>> channelFrames;
  private ListeningExecutorService innerExec;
  private FrameProcessorExecutor exec;
  private List<ReadableFrameChannel> inputChannels;
  private File tempDir;

  private final AtomicLong intermediateChannelsOpened = new AtomicLong();
  private long invocations;

  @Setup(Level.Trial)
  public void setupTrial()
  {
    signature = FrameWriters.sortableSignature(
        RowSignature.builder().add(KEY, ColumnType.STRING).add(VALUE, ColumnType.STRING).build(),
        sortKey
    );
    frameReader = FrameReader.create(signature);
    exec = new FrameProcessorExecutor(
        innerExec = MoreExecutors.listeningDecorator(
            Execs.multiThreaded(maxActiveProcessors, StringUtils.encodeForFormat(getClass().getSimpleName()) + "-%d")
        )
    );

    final Random random = new Random(0);
    final String value = " ".repeat(ROW_LENGTH - KEY_LENGTH);
    final List<String[]> rows = new ArrayList<>(scale.numRows);
    for (int i = 0; i < scale.numRows; i++) {
      final StringBuilder key = new StringBuilder(KEY_LENGTH);
      for (int j = 0; j < KEY_LENGTH; j++) {
        key.append((char) ('a' + random.nextInt(26)));
      }
      rows.add(new String[]{key.toString(), value});
    }

    final RowBasedSegment<String[]> segment = new RowBasedSegment<>(
        Sequences.simple(rows),
        columnName -> {
          if (KEY.equals(columnName)) {
            return row -> row[0];
          } else if (VALUE.equals(columnName)) {
            return row -> row[1];
          } else if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
            return row -> 0L;
          } else {
            throw new ISE("No such column[%s]", columnName);
          }
        },
        signature
    );

    channelFrames = new ArrayList<>();
    for (int i = 0; i < NUM_CHANNELS; i++) {
      channelFrames.add(new ArrayList<>());
    }

    final List<Frame> frames = FrameSequenceBuilder.fromCursorFactory(segment.as(CursorFactory.class))
                                                   .allocator(ArenaMemoryAllocator.createOnHeap(scale.frameSize))
                                                   .sortBy(sortKey)
                                                   .frameType(FrameType.latestRowBased())
                                                   .frames()
                                                   .toList();
    for (int i = 0; i < frames.size(); i++) {
      channelFrames.get(i % NUM_CHANNELS).add(frames.get(i));
    }
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException
  {
    tempDir = FileUtils.createTempDir();
    inputChannels = new ArrayList<>(NUM_CHANNELS);
    for (final List<Frame> frames : channelFrames) {
      final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(frames.size());
      for (final Frame frame : frames) {
        channel.writable().write(frame);
      }
      channel.writable().close();
      inputChannels.add(channel.readable());
    }
  }

  @TearDown(Level.Invocation)
  public void tearDownInvocation() throws IOException
  {
    FileUtils.deleteDirectory(tempDir);
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws Exception
  {
    System.out.printf(
        "%nintermediate channels opened per invocation: %.1f%n",
        (double) intermediateChannelsOpened.get() / invocations
    );

    innerExec.shutdownNow();
    if (!innerExec.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("Could not terminate executor after 1 minute");
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sort(final Blackhole blackhole) throws Exception
  {
    final WireTransferableContext wireTransferableContext = new WireTransferableContext(null, null, true);
    final OutputChannelFactory intermediateOutputChannelFactory = new FileOutputChannelFactory(
        new File(tempDir, "intermediate"),
        scale.frameSize,
        null,
        wireTransferableContext
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

    final SuperSorter superSorter = new SuperSorter(
        inputChannels,
        frameReader,
        sortKey,
        Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
        exec,
        FrameProcessorDecorator.NONE,
        new FileOutputChannelFactory(new File(tempDir, "output"), scale.frameSize, null, wireTransferableContext),
        intermediateOutputChannelFactory,
        FrameType.latestRowBased(),
        maxActiveProcessors,
        scale.maxChannelsPerMerger,
        scale.frameSize,
        rowLimit,
        null,
        new SuperSorterProgressTracker(),
        false,
        null
    );

    long rowsRead = 0;
    for (final OutputChannel outputChannel : superSorter.run().get().getAllChannels()) {
      final ReadableFrameChannel channel = outputChannel.getReadableChannel();
      while (!channel.isFinished()) {
        channel.readabilityFuture().get();
        if (channel.canRead()) {
          final Frame frame = channel.readFrame();
          rowsRead += frame.numRows();
          blackhole.consume(frame);
        }
      }
      channel.close();
    }

    final long expectedRows = rowLimit == SuperSorter.UNLIMITED ? scale.numRows : Math.min(rowLimit, scale.numRows);
    if (rowsRead != expectedRows) {
      throw new ISE("Read rows[%d], expected[%d]", rowsRead, expectedRows);
    }

    invocations++;
  }
}
