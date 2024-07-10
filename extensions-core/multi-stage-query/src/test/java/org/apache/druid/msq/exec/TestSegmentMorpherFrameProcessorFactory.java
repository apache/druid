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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorFrameProcessor;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.kernel.ExtraInfoHolder;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A segment morpher which does nothing but updates the version of the segment by touching it.
 */
@JsonTypeName("testSegmentMorpherFrameProcessorFactory")
public class TestSegmentMorpherFrameProcessorFactory implements FrameProcessorFactory<DataSegment, Set<DataSegment>, Object>
{
  @Override
  public ProcessorsAndChannels<DataSegment, Set<DataSegment>> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher,
      boolean removeNullBytes
  )
  {
    if (inputSlices.get(0) instanceof NilInputSlice) {
      return new ProcessorsAndChannels<>(
          ProcessorManagers.of(Sequences.<SegmentGeneratorFrameProcessor>empty())
                           .withAccumulation(new HashSet<>(), (acc, segment) -> acc),
          OutputChannels.none()
      );
    }

    final SegmentsInputSlice segmentsSlice = (SegmentsInputSlice) inputSlices.get(0);
    final ReadableInputs segments =
        inputSliceReader.attach(0, segmentsSlice, counters, warningPublisher);

    Sequence<MorphFrameProcessor> sequence = Sequences.simple(segments)
                                                 .map(segment -> {
                                                   final RichSegmentDescriptor descriptor = segment.getSegment()
                                                                                                   .getDescriptor();
                                                   final SegmentId sourceSegmentId = SegmentId.of(
                                                       segmentsSlice.getDataSource(),
                                                       descriptor.getFullInterval(),
                                                       descriptor.getVersion(),
                                                       descriptor.getPartitionNumber()
                                                   );

                                                   final DataSegment newDataSegment = new DataSegment(
                                                       sourceSegmentId.getDataSource(),
                                                       sourceSegmentId.getInterval(),
                                                       MSQTestTaskActionClient.VERSION,
                                                       ImmutableMap.of(),
                                                       Collections.emptyList(),
                                                       Collections.emptyList(),
                                                       new NumberedShardSpec(0, 1),
                                                       null,
                                                       0
                                                   );
                                                   return new MorphFrameProcessor(newDataSegment);
                                                 });

    final ProcessorManager<DataSegment, Set<DataSegment>> processorManager =
        ProcessorManagers.of(sequence)
                         .withAccumulation(
                             new HashSet<>(),
                             (set, segment) -> {
                               set.add(segment);
                               return set;
                             }
                         );
    return new ProcessorsAndChannels<>(processorManager, OutputChannels.none());
  }

  @Nullable
  @Override
  public TypeReference<Set<DataSegment>> getResultTypeReference()
  {
    return new TypeReference<Set<DataSegment>>() {};
  }

  @Override
  public Set<DataSegment> mergeAccumulatedResult(Set<DataSegment> accumulated, Set<DataSegment> otherAccumulated)
  {
    return Collections.emptySet();
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    Assert.assertEquals(MSQTestTaskActionClient.VERSION, extra);
    return new ExtraInfoHolder(extra)
    {
    };
  }

  private static class MorphFrameProcessor implements FrameProcessor<DataSegment>
  {
    private final DataSegment segment;

    public MorphFrameProcessor(DataSegment segment)
    {
      this.segment = segment;
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
    public ReturnOrAwait<DataSegment> runIncrementally(IntSet readableInputs)
    {
      return ReturnOrAwait.returnObject(segment);
    }

    @Override
    public void cleanup()
    {
    }
  }
}
