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

package org.apache.druid.msq.querykit.results;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.ExtraInfoHolder;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.NilExtraInfoHolder;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

@JsonTypeName("exportResults")
public class ExportResultsFrameProcessorFactory implements FrameProcessorFactory<String, Set<String>,Object>
{
  private final String queryId;
  private final ExportStorageProvider exportStorageProvider;
  private final ResultFormat exportFormat;

  @JsonCreator
  public ExportResultsFrameProcessorFactory(
      @JsonProperty("queryId") String queryId,
      @JsonProperty("exportStorageProvider") ExportStorageProvider exportStorageProvider,
      @JsonProperty("exportFormat") ResultFormat exportFormat
  )
  {
    this.queryId = queryId;
    this.exportStorageProvider = exportStorageProvider;
    this.exportFormat = exportFormat;
  }

  @JsonProperty("queryId")
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty("exportFormat")
  public ResultFormat getExportFormat()
  {
    return exportFormat;
  }


  @JsonProperty("exportStorageProvider")
  public ExportStorageProvider getExportStorageProvider()
  {
    return exportStorageProvider;
  }

  @Override
  public ProcessorsAndChannels<String, Set<String>> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    final StageInputSlice slice = (StageInputSlice) CollectionUtils.getOnlyElement(
        inputSlices,
        x -> DruidException.defensive().build("Expected only a single input slice but found [%s]", inputSlices)
    );

    if (inputSliceReader.numReadableInputs(slice) == 0) {
      return new ProcessorsAndChannels<>(
          ProcessorManagers.of(Sequences.<ExportResultsFrameProcessor>empty())
                           .withAccumulation(new HashSet<>(), (acc, file) -> {
                             acc.add(file);
                             return acc;
                           }),
          OutputChannels.none()
      );
    }

    ChannelCounters channelCounter = counters.channel(CounterNames.outputChannel());
    final Sequence<ReadableInput> readableInputs =
        Sequences.simple(inputSliceReader.attach(0, slice, counters, warningPublisher));

    final Sequence<FrameProcessor<String>> processors = readableInputs.map(
        readableInput -> new ExportResultsFrameProcessor(
            readableInput.getChannel(),
            exportFormat,
            readableInput.getChannelFrameReader(),
            exportStorageProvider.get(),
            frameContext.jsonMapper(),
            channelCounter,
            getExportFilePath(queryId, workerNumber, readableInput.getStagePartition().getPartitionNumber(), exportFormat)
        )
    );

    return new ProcessorsAndChannels<>(
        ProcessorManagers.of(processors)
                         .withAccumulation(new HashSet<>(), (acc, file) -> {
                           acc.add(file);
                           return acc;
                         }),
        OutputChannels.none()
    );
  }

  @Nullable
  @Override
  public TypeReference<Set<String>> getResultTypeReference()
  {
    return new TypeReference<Set<String>>() {};
  }

  @Override
  public Set<String> mergeAccumulatedResult(Set<String> accumulated, Set<String> otherAccumulated)
  {
    accumulated.addAll(otherAccumulated);
    return accumulated;
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    if (extra != null) {
      throw new ISE("Expected null 'extra'");
    }

    return NilExtraInfoHolder.instance();
  }

  private static String getExportFilePath(String queryId, int workerNumber, int partitionNumber, ResultFormat exportFormat)
  {
    return StringUtils.format("%s-worker%s-partition%s.%s", queryId, workerNumber, partitionNumber, exportFormat.toString());
  }
}
