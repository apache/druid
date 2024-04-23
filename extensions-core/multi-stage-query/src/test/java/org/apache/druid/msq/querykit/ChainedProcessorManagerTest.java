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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.FrameProcessorExecutorTest;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.processor.manager.SequenceProcessorManagerTest;
import org.apache.druid.frame.processor.test.SimpleReturningFrameProcessor;
import org.apache.druid.frame.processor.test.SingleChannelFrameProcessor;
import org.apache.druid.frame.processor.test.SingleRowWritingFrameProcessor;
import org.apache.druid.frame.processor.test.TestFrameProcessorUtils;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link ChainedProcessorManager}.
 */
@RunWith(Parameterized.class)
public class ChainedProcessorManagerTest extends FrameProcessorExecutorTest.BaseFrameProcessorExecutorTestSuite
{
  private final int bouncerPoolSize;
  private final int maxOutstandingProcessors;

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                               .addTimeColumn()
                                                               .add("col", ColumnType.LONG)
                                                               .build();

  public ChainedProcessorManagerTest(int numThreads, int bouncerPoolSize, int maxOutstandingProcessors)
  {
    super(numThreads);
    this.bouncerPoolSize = bouncerPoolSize;
    this.maxOutstandingProcessors = maxOutstandingProcessors;
  }

  @Parameterized.Parameters(name = "numThreads = {0}, bouncerPoolSize = {1}, maxOutstandingProcessors = {2}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (int numThreads : new int[]{1, 3, 12}) {
      for (int bouncerPoolSize : new int[]{1, 3, 12, Integer.MAX_VALUE}) {
        for (int maxOutstandingProcessors : new int[]{1, 3, 12}) {
          constructors.add(new Object[]{numThreads, bouncerPoolSize, maxOutstandingProcessors});
        }
      }
    }

    return constructors;
  }

  @Test
  public void test_simple_single_threaded_chaining() throws ExecutionException, InterruptedException
  {
    final ImmutableList<Long> expectedValues = ImmutableList.of(1L, 2L, 3L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(3);
    final SimpleReturningFrameProcessor<List<Long>> realtimeProcessor = new SimpleReturningFrameProcessor<>(
        expectedValues
    );

    final ChainedProcessorManager<List<Long>, Long, Long> chainedProcessorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(() -> realtimeProcessor),
        (values) -> createNextProcessors(outputChannel.writable(), values.stream().flatMap(List::stream).collect(Collectors.toList()))
    );

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        maxOutstandingProcessors,
        new Bouncer(bouncerPoolSize),
        null
    );

    future.get();

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(new HashSet<>(expectedValues), actualValues);
  }

  @Test
  public void test_multiple_processor_manager() throws ExecutionException, InterruptedException
  {
    final ImmutableSet<Long> expectedValues = ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ChainedProcessorManager<List<Long>, Long, Long> chainedProcessorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(
            ImmutableList.of(
                new SimpleReturningFrameProcessor<>(ImmutableList.of(1L, 2L, 3L)),
                new SimpleReturningFrameProcessor<>(ImmutableList.of(4L, 5L, 6L))
                )
        ),
        (values) -> createNextProcessors(outputChannel.writable(), values.stream().flatMap(List::stream).collect(Collectors.toList()))
    );

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        3,
        new Bouncer(3),
        null
    );

    future.get();

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(expectedValues, actualValues);
  }

  @Test
  public void test_failing_processor_manager()
  {
    final ImmutableSet<Long> expectedValues = ImmutableSet.of();
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ChainedProcessorManager<List<Long>, Long, Long> chainedProcessorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(
            ImmutableList.of(
                new SimpleReturningFrameProcessor<>(ImmutableList.of(4L, 5L, 6L)),
                new SequenceProcessorManagerTest.NilFrameProcessor<>()
            )
        ),
        (values) -> createNextProcessors(outputChannel.writable(), values.stream().flatMap(List::stream).collect(Collectors.toList()))
    );

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        3,
        new Bouncer(3),
        null
    );

    Assert.assertThrows(ExecutionException.class, future::get);

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(expectedValues, actualValues);
  }

  @Test
  public void test_chaining_processor_manager() throws InterruptedException, ExecutionException
  {
    final ImmutableSet<Long> expectedValues = ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ProcessorManager<List<Object>, Long> chainedProcessorManager =
        chainedProcessors(outputChannel.writable(), ImmutableList.of(new ArrayList<>(expectedValues)), 10L);

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        3,
        new Bouncer(3),
        null
    );

    future.get();

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(expectedValues, actualValues);
  }

  @Test
  public void test_chaining_processor_manager_with_exception()
  {
    final ImmutableSet<Long> expectedValues = ImmutableSet.of(1L, 2L, 3L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ProcessorManager<List<Object>, Long> chainedProcessorManager =
        chainedProcessors(outputChannel.writable(), ImmutableList.of(new ArrayList<>(ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L))), 4L);

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        3,
        new Bouncer(3),
        null
    );

    Assert.assertThrows(ExecutionException.class, future::get);

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(expectedValues, actualValues);
  }

  private Long extractColumnValue(Frame frame, int columnNo)
  {
    FrameReader frameReader = FrameReader.create(ROW_SIGNATURE);
    FrameCursor frameCursor = FrameProcessors.makeCursor(frame, frameReader);
    ColumnSelectorFactory columnSelectorFactory = frameCursor.getColumnSelectorFactory();
    ColumnValueSelector<Long> columnValueSelector = columnSelectorFactory.makeColumnValueSelector(ROW_SIGNATURE.getColumnName(columnNo));
    return columnValueSelector.getLong();
  }

  private ProcessorManager<Long, Long> createNextProcessors(WritableFrameChannel frameChannel, List<Long> values)
  {
    List<SingleRowWritingFrameProcessor> processors = new ArrayList<>();

    for (Long value : values) {
      processors.add(new SingleRowWritingFrameProcessor(frameChannel, makeInputRow(ROW_SIGNATURE.getColumnName(1), value)));
    }

    return ProcessorManagers.of(processors);
  }

  private static InputRow makeInputRow(Object... kv)
  {
    final Map<String, Object> event = TestHelper.makeMap(true, kv);
    return new MapBasedInputRow(0L, ImmutableList.copyOf(event.keySet()), event);
  }

  public static class OutputHeadFrameProcessor extends SingleChannelFrameProcessor<List<Long>> // TODO: rename this
  {
    private final List<Long> valuesList;
    private final Long failureValue;
    private final WritableFrameChannel outputChannel;

    public OutputHeadFrameProcessor(WritableFrameChannel outputChannel, List<Long> valuesList, Long failureValue)
    {
      super(null, null);
      this.outputChannel = outputChannel;
      this.valuesList = valuesList;
      this.failureValue = failureValue;
    }

    @Override
    public List<ReadableFrameChannel> inputChannels()
    {
      return Collections.emptyList();
    }

    @Override
    public List<WritableFrameChannel> outputChannels()
    {
      return Collections.emptyList();
    }

    @Override
    public List<Long> doSimpleWork() throws IOException
    {
      Long l = valuesList.get(0);
      if (failureValue.equals(l)) {
        throw new RuntimeException();
      }
      InputRow inputRow = makeInputRow(ROW_SIGNATURE.getColumnName(1), l);
      outputChannel.write(TestFrameProcessorUtils.toFrame(Collections.singletonList(inputRow)));
      if (valuesList.size() == 1) {
        return Collections.emptyList();
      }
      return valuesList.subList(1, valuesList.size());
    }
  }

  public static ProcessorManager<List<Object>, Long> chainedProcessors(WritableFrameChannel writableFrameChannel, List<List<Long>> values, Long failureValue)
  {
    List<Long> value = values.get(0); // TODO: not just the first element.
    if (value.isEmpty()) {
      return ProcessorManagers.none();
    }
    ChainedProcessorManager<List<Long>, List<Long>, Long> processorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(() -> new OutputHeadFrameProcessor(writableFrameChannel, value, failureValue)),
        lists -> (ProcessorManager) chainedProcessors(
            writableFrameChannel,
            lists,
            failureValue
        )
    );
    return (ProcessorManager) processorManager;
  }
}
