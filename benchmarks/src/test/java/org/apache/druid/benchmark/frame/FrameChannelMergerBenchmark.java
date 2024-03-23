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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FrameChannelMerger;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Benchmark for {@link FrameChannelMerger}.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class FrameChannelMergerBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final String KEY = "key";
  private static final String VALUE = "value";

  @Param({"5000000"})
  private int numRows;

  @Param({"2", "16"})
  private int numChannels;

  @Param({"20"})
  private int keyLength;

  @Param({"100"})
  private int rowLength;

  /**
   * Linked to {@link KeyGenerator}.
   */
  @Param({"random", "sequential"})
  private String keyGeneratorString;

  /**
   * Linked to {@link ChannelDistribution}.
   */
  @Param({"round_robin", "clustered"})
  private String channelDistributionString;

  /**
   * Generator of keys.
   */
  enum KeyGenerator
  {
    /**
     * Random characters from a-z.
     */
    RANDOM {
      @Override
      public String generateKey(int rowNumber, int keyLength)
      {
        final StringBuilder builder = new StringBuilder(keyLength);
        for (int i = 0; i < keyLength; i++) {
          builder.append((char) ('a' + ThreadLocalRandom.current().nextInt(26)));
        }
        return builder.toString();
      }
    },

    /**
     * Sequential with zero-padding.
     */
    SEQUENTIAL {
      @Override
      public String generateKey(int rowNumber, int keyLength)
      {
        return StringUtils.format("%0" + keyLength + "d", rowNumber);
      }
    };

    public abstract String generateKey(int rowNumber, int keyLength);
  }

  /**
   * Distribution of rows across channels.
   */
  enum ChannelDistribution
  {
    /**
     * Sequential keys are distributed round-robin to channels.
     */
    ROUND_ROBIN {
      @Override
      public int getChannelNumber(int rowNumber, int numRows, int numChannels)
      {
        return rowNumber % numChannels;
      }
    },

    /**
     * Sequential keys are clustered into the same channels.
     */
    CLUSTERED {
      @Override
      public int getChannelNumber(int rowNumber, int numRows, int numChannels)
      {
        final int rowsPerChannel = numRows / numChannels;
        return rowNumber / rowsPerChannel;
      }
    };

    public abstract int getChannelNumber(int rowNumber, int numRows, int numChannels);
  }

  private final RowSignature signature =
      RowSignature.builder()
                  .add(KEY, ColumnType.STRING)
                  .add(VALUE, ColumnType.STRING)
                  .build();

  private final FrameReader frameReader = FrameReader.create(signature);
  private final List<KeyColumn> sortKey = ImmutableList.of(new KeyColumn(KEY, KeyOrder.ASCENDING));

  private List<List<Frame>> channelFrames;
  private FrameProcessorExecutor exec;
  private List<BlockingQueueFrameChannel> channels;

  /**
   * Create {@link #numChannels} channels in {@link #channels}, with {@link #numRows} total rows split across the
   * channels according to {@link ChannelDistribution}. Each channel is individually sorted, as required
   * by {@link FrameChannelMerger}.
   *
   * Rows are fixed-length at {@link #rowLength} with fixed-length keys at {@link #keyLength}. Keys are generated
   * by {@link KeyGenerator}.
   */
  @Setup(Level.Trial)
  public void setupTrial()
  {
    exec = new FrameProcessorExecutor(
        MoreExecutors.listeningDecorator(
            Execs.singleThreaded(StringUtils.encodeForFormat(getClass().getSimpleName()))
        )
    );

    final KeyGenerator keyGenerator = KeyGenerator.valueOf(StringUtils.toUpperCase(keyGeneratorString));
    final ChannelDistribution channelDistribution =
        ChannelDistribution.valueOf(StringUtils.toUpperCase(channelDistributionString));

    // Create channelRows which holds rows for each channel.
    final List<List<NonnullPair<String, String>>> channelRows = new ArrayList<>();
    channelFrames = new ArrayList<>();
    for (int channelNumber = 0; channelNumber < numChannels; channelNumber++) {
      channelRows.add(new ArrayList<>());
      channelFrames.add(new ArrayList<>());
    }

    // Create "valueString", a string full of spaces to pad out the row.
    final StringBuilder valueStringBuilder = new StringBuilder();
    for (int i = 0; i < rowLength - keyLength; i++) {
      valueStringBuilder.append(' ');
    }
    final String valueString = valueStringBuilder.toString();

    // Populate "channelRows".
    for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
      final String keyString = keyGenerator.generateKey(rowNumber, keyLength);
      final NonnullPair<String, String> row = new NonnullPair<>(keyString, valueString);
      channelRows.get(channelDistribution.getChannelNumber(rowNumber, numRows, numChannels)).add(row);
    }

    // Sort each "channelRows".
    for (List<NonnullPair<String, String>> rows : channelRows) {
      rows.sort(Comparator.comparing(row -> row.lhs));
    }

    // Populate each "channelFrames".
    for (int channelNumber = 0; channelNumber < numChannels; channelNumber++) {
      final List<NonnullPair<String, String>> rows = channelRows.get(channelNumber);
      final RowBasedSegment<NonnullPair<String, String>> segment =
          new RowBasedSegment<>(
              SegmentId.dummy("__dummy"),
              Sequences.simple(rows),
              columnName -> {
                if (KEY.equals(columnName)) {
                  return row -> row.lhs;
                } else if (VALUE.equals(columnName)) {
                  return row -> row.rhs;
                } else if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
                  return row -> 0L;
                } else {
                  throw new ISE("No such column[%s]", columnName);
                }
              },
              signature
          );
      final Sequence<Frame> frameSequence =
          FrameSequenceBuilder.fromAdapter(segment.asStorageAdapter())
                              .allocator(ArenaMemoryAllocator.createOnHeap(10_000_000))
                              .frameType(FrameType.ROW_BASED)
                              .frames();
      final List<Frame> channelFrameList = channelFrames.get(channelNumber);
      frameSequence.forEach(channelFrameList::add);
      rows.clear();
    }
  }

  /**
   * Create {@link #numChannels} channels in {@link #channels}, with {@link #numRows} total rows split across the
   * channels according to {@link ChannelDistribution}. Each channel is individually sorted, as required
   * by {@link FrameChannelMerger}.
   *
   * Rows are fixed-length at {@link #rowLength} with fixed-length keys at {@link #keyLength}. Keys are generated
   * by {@link KeyGenerator}.
   */
  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException
  {
    exec = new FrameProcessorExecutor(
        MoreExecutors.listeningDecorator(
            Execs.singleThreaded(StringUtils.encodeForFormat(getClass().getSimpleName()))
        )
    );

    // Create channels.
    channels = new ArrayList<>(numChannels);
    for (int channelNumber = 0; channelNumber < numChannels; channelNumber++) {
      channels.add(new BlockingQueueFrameChannel(100));
    }

    // Populate each channel.
    for (int channelNumber = 0; channelNumber < numChannels; channelNumber++) {
      final List<Frame> frames = channelFrames.get(channelNumber);
      final WritableFrameChannel writableChannel = channels.get(channelNumber).writable();
      for (Frame frame : frames) {
        writableChannel.write(frame);
      }
    }

    // Close all channels.
    for (BlockingQueueFrameChannel channel : channels) {
      channel.writable().close();
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    exec.getExecutorService().shutdownNow();
    if (!exec.getExecutorService().awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("Could not terminate executor after 1 minute");
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void mergeChannels(Blackhole blackhole)
  {
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();
    final FrameChannelMerger merger = new FrameChannelMerger(
        channels.stream().map(BlockingQueueFrameChannel::readable).collect(Collectors.toList()),
        frameReader,
        outputChannel.writable(),
        FrameWriters.makeFrameWriterFactory(
            FrameType.ROW_BASED,
            new ArenaMemoryAllocatorFactory(1_000_000),
            signature,
            sortKey
        ),
        sortKey,
        null,
        -1
    );

    final ListenableFuture<Long> retVal = exec.runFully(merger, null);

    while (!outputChannel.readable().isFinished()) {
      FutureUtils.getUnchecked(outputChannel.readable().readabilityFuture(), false);
      if (outputChannel.readable().canRead()) {
        final Frame frame = outputChannel.readable().read();
        blackhole.consume(frame);
      }
    }

    if (FutureUtils.getUnchecked(retVal, true) != numRows) {
      throw new ISE("Incorrect numRows[%s], expected[%s]", FutureUtils.getUncheckedImmediately(retVal), numRows);
    }
  }
}
