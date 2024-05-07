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
import com.google.common.collect.Iterables;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Unit tests for {@link ChainedProcessorManager}.
 */
@RunWith(Parameterized.class)
public class ChainedProcessorManagerTest extends FrameProcessorExecutorTest.BaseFrameProcessorExecutorTestSuite
{
  private final Bouncer bouncer;
  private final int maxOutstandingProcessors;

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                               .addTimeColumn()
                                                               .add("col", ColumnType.LONG)
                                                               .build();

  public ChainedProcessorManagerTest(int numThreads, int bouncerPoolSize, int maxOutstandingProcessors)
  {
    super(numThreads);
    this.bouncer = bouncerPoolSize == Integer.MAX_VALUE ? Bouncer.unlimited() : new Bouncer(bouncerPoolSize);
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

  /**
   * Simple functional test of the chained processor manager.
   * The {@link ChainedProcessorManager#first} is a {@link SimpleReturningFrameProcessor} which returns the list of
   * values. The {@link ChainedProcessorManager#restFactory} is a function that creates a
   * {@link SingleRowWritingFrameProcessor} for each value.
   */
  @Test
  public void test_simple_chained_processor() throws ExecutionException, InterruptedException
  {
    final ImmutableList<Long> expectedValues = ImmutableList.of(1L, 2L, 3L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(3);
    final SimpleReturningFrameProcessor<List<Long>> firstProcessor = new SimpleReturningFrameProcessor<>(expectedValues);

    final ChainedProcessorManager<List<Long>, Long, Long> chainedProcessorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(() -> firstProcessor),
        (values) -> createNextProcessors(outputChannel.writable(), values.stream().flatMap(List::stream).collect(Collectors.toList()))
    );

    exec.runAllFully(
        chainedProcessorManager,
        maxOutstandingProcessors,
        bouncer,
        null
    ).get();

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(new HashSet<>(expectedValues), actualValues);
  }

  /**
   * Test with multiple processors in {@link ChainedProcessorManager#first}.
   */
  @Test
  public void test_multiple_processor_manager() throws ExecutionException, InterruptedException
  {
    final ImmutableList<Long> valueSet1 = ImmutableList.of(1L, 2L, 3L);
    final ImmutableList<Long> valueSet2 = ImmutableList.of(4L, 5L, 6L);
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ChainedProcessorManager<List<Long>, Long, Long> chainedProcessorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(
            ImmutableList.of(
                new SimpleReturningFrameProcessor<>(valueSet1),
                new SimpleReturningFrameProcessor<>(valueSet2)
            )
        ),
        (values) -> createNextProcessors(outputChannel.writable(), values.stream().flatMap(List::stream).collect(Collectors.toList()))
    );

    exec.runAllFully(
        chainedProcessorManager,
        maxOutstandingProcessors,
        bouncer,
        null
    ).get();

    final Set<Long> expectedValues = new HashSet<>();
    expectedValues.addAll(valueSet1);
    expectedValues.addAll(valueSet2);
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
        (values) -> createNextProcessors(
            new NonFailingWritableFrameChannel(outputChannel.writable()),
            values.stream().flatMap(List::stream).collect(Collectors.toList())
        )
    );

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        maxOutstandingProcessors,
        bouncer,
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
  public void test_chaining_processor_manager()
  {
    final Set<Long> values = LongStream.range(1, 10).boxed().collect(Collectors.toSet());
    runChainedExceptionTest(values, values, null);
  }

  @Test
  public void test_chaining_processor_manager_with_exception()
  {
    final Set<Long> values = LongStream.range(1, 10).boxed().collect(Collectors.toSet());

    for (long failingValue = 1; failingValue < 10; failingValue++) {
      final Set<Long> expectedValues = LongStream.range(1, failingValue).boxed().collect(Collectors.toSet());
      runChainedExceptionTest(values, expectedValues, failingValue);
    }
  }

  public void runChainedExceptionTest(Set<Long> values, Set<Long> expectedValues, Long failingValue)
  {
    final BlockingQueueFrameChannel outputChannel = new BlockingQueueFrameChannel(20);

    final ProcessorManager<List<Object>, Long> chainedProcessorManager =
        chainedProcessors(outputChannel.writable(), new ArrayList<>(values), failingValue);

    final ListenableFuture<Long> future = exec.runAllFully(
        chainedProcessorManager,
        maxOutstandingProcessors,
        bouncer,
        null
    );

    try {
      future.get();
    }
    catch (Exception ignored) {
    }

    final HashSet<Long> actualValues = new HashSet<>();
    try (ReadableFrameChannel readable = outputChannel.readable()) {
      while (readable.canRead()) {
        actualValues.add(extractColumnValue(readable.read(), 1));
      }
    }
    Assert.assertEquals(expectedValues, actualValues);
  }

  @SuppressWarnings("rawtypes")
  private Long extractColumnValue(Frame frame, int columnNo)
  {
    FrameReader frameReader = FrameReader.create(ROW_SIGNATURE);
    FrameCursor frameCursor = FrameProcessors.makeCursor(frame, frameReader);
    ColumnSelectorFactory columnSelectorFactory = frameCursor.getColumnSelectorFactory();
    ColumnValueSelector columnValueSelector = columnSelectorFactory.makeColumnValueSelector(ROW_SIGNATURE.getColumnName(columnNo));
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

  public static class PrintFirstAndReturnRestFrameProcessor extends SingleChannelFrameProcessor<List<Long>>
  {
    private final List<Long> valuesList;
    private final Long failureValue;

    public PrintFirstAndReturnRestFrameProcessor(WritableFrameChannel outputChannel, List<Long> valuesList, Long failureValue)
    {
      super(null, new NonFailingWritableFrameChannel(outputChannel));
      this.valuesList = valuesList;
      this.failureValue = failureValue;
    }

    @Override
    public List<Long> doSimpleWork() throws IOException
    {
      Long firstValue = valuesList.get(0);
      if (Objects.equals(firstValue, failureValue)) {
        throw new RuntimeException();
      }
      InputRow inputRow = makeInputRow(ROW_SIGNATURE.getColumnName(1), firstValue);
      Iterables.getOnlyElement(outputChannels()).write(TestFrameProcessorUtils.toFrame(Collections.singletonList(inputRow)));
      if (valuesList.size() == 1) {
        return Collections.emptyList();
      }
      return valuesList.subList(1, valuesList.size());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static ProcessorManager<List<Object>, Long> chainedProcessors(
      WritableFrameChannel writableFrameChannel,
      List<Long> values,
      Long failureValue
  )
  {
    if (values.isEmpty()) {
      return ProcessorManagers.none();
    }
    ChainedProcessorManager<List<Long>, List<Long>, Long> processorManager = new ChainedProcessorManager<>(
        ProcessorManagers.of(() -> new PrintFirstAndReturnRestFrameProcessor(writableFrameChannel, values, failureValue)),
        returnedValues -> {
          List<Long> lists = returnedValues.stream().flatMap(List::stream).collect(Collectors.toList());
          return (ProcessorManager) chainedProcessors(
              writableFrameChannel,
              lists,
              failureValue
          );
        }
    );
    return (ProcessorManager) processorManager;
  }
}
