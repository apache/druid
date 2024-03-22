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

package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshotsSerializer;
import org.apache.druid.msq.counters.SegmentGenerationProgressCounter;
import org.apache.druid.msq.counters.SuperSorterProgressTrackerCounter;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.error.BroadcastTablesTooLargeFault;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.ColumnNameRestrictedFault;
import org.apache.druid.msq.indexing.error.ColumnTypeNotSupportedFault;
import org.apache.druid.msq.indexing.error.DurableStorageConfigurationFault;
import org.apache.druid.msq.indexing.error.InsertCannotAllocateSegmentFault;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.InsertTimeNullFault;
import org.apache.druid.msq.indexing.error.InsertTimeOutOfBoundsFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.NotEnoughTemporaryStorageFault;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.error.QueryRuntimeFault;
import org.apache.druid.msq.indexing.error.RowTooLargeFault;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForJob;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.druid.msq.indexing.error.TooManyBucketsFault;
import org.apache.druid.msq.indexing.error.TooManyClusteredByColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyInputFilesFault;
import org.apache.druid.msq.indexing.error.TooManyPartitionsFault;
import org.apache.druid.msq.indexing.error.TooManyRowsWithSameKeyFault;
import org.apache.druid.msq.indexing.error.TooManyWarningsFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorFrameProcessorFactory;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.NilInputSource;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.input.inline.InlineInputSlice;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.lookup.LookupInputSlice;
import org.apache.druid.msq.input.lookup.LookupInputSpec;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.NilExtraInfoHolder;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.common.OffsetLimitFrameProcessorFactory;
import org.apache.druid.msq.querykit.common.SortMergeJoinFrameProcessorFactory;
import org.apache.druid.msq.querykit.groupby.GroupByPostShuffleFrameProcessorFactory;
import org.apache.druid.msq.querykit.groupby.GroupByPreShuffleFrameProcessorFactory;
import org.apache.druid.msq.querykit.results.ExportResultsFrameProcessorFactory;
import org.apache.druid.msq.querykit.results.QueryResultFrameProcessorFactory;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.msq.util.PassthroughAggregatorFactory;
import org.apache.druid.query.DruidProcessingConfig;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Module that adds {@link MSQControllerTask}, {@link MSQWorkerTask}, and dependencies.
 */
public class MSQIndexingModule implements DruidModule
{
  static final String BASE_MSQ_KEY = "druid.msq";

  public static final List<Class<? extends MSQFault>> FAULT_CLASSES = ImmutableList.of(
      BroadcastTablesTooLargeFault.class,
      CanceledFault.class,
      CannotParseExternalDataFault.class,
      ColumnTypeNotSupportedFault.class,
      ColumnNameRestrictedFault.class,
      DurableStorageConfigurationFault.class,
      InsertCannotAllocateSegmentFault.class,
      InsertCannotBeEmptyFault.class,
      InsertLockPreemptedFault.class,
      InsertTimeNullFault.class,
      InsertTimeOutOfBoundsFault.class,
      InvalidNullByteFault.class,
      NotEnoughTemporaryStorageFault.class,
      NotEnoughMemoryFault.class,
      QueryNotSupportedFault.class,
      QueryRuntimeFault.class,
      RowTooLargeFault.class,
      TaskStartTimeoutFault.class,
      TooManyBucketsFault.class,
      TooManyClusteredByColumnsFault.class,
      TooManyColumnsFault.class,
      TooManyInputFilesFault.class,
      TooManyPartitionsFault.class,
      TooManyRowsWithSameKeyFault.class,
      TooManyWarningsFault.class,
      TooManyWorkersFault.class,
      TooManyAttemptsForJob.class,
      UnknownFault.class,
      WorkerFailedFault.class,
      TooManyAttemptsForWorker.class,
      WorkerRpcFailedFault.class
  );

  @Override
  public List<? extends Module> getJacksonModules()
  {
    final SimpleModule module = new SimpleModule(getClass().getSimpleName());

    module.registerSubtypes(
        // Task classes
        MSQControllerTask.class,
        MSQWorkerTask.class,

        // FrameChannelWorkerFactory and FrameChannelWorkerFactoryExtraInfoHolder classes
        SegmentGeneratorFrameProcessorFactory.class,
        SegmentGeneratorFrameProcessorFactory.SegmentGeneratorExtraInfoHolder.class,
        ScanQueryFrameProcessorFactory.class,
        GroupByPreShuffleFrameProcessorFactory.class,
        GroupByPostShuffleFrameProcessorFactory.class,
        OffsetLimitFrameProcessorFactory.class,
        NilExtraInfoHolder.class,
        SortMergeJoinFrameProcessorFactory.class,
        QueryResultFrameProcessorFactory.class,
        ExportResultsFrameProcessorFactory.class,

        // DataSource classes (note: ExternalDataSource is in MSQSqlModule)
        InputNumberDataSource.class,

        // TaskReport classes
        MSQTaskReport.class,

        // QueryCounter.Snapshot classes
        ChannelCounters.Snapshot.class,
        SuperSorterProgressTrackerCounter.Snapshot.class,
        WarningCounters.Snapshot.class,
        SegmentGenerationProgressCounter.Snapshot.class,

        // InputSpec classes
        ExternalInputSpec.class,
        InlineInputSpec.class,
        LookupInputSpec.class,
        StageInputSpec.class,
        TableInputSpec.class,

        // InputSlice classes
        ExternalInputSlice.class,
        InlineInputSlice.class,
        LookupInputSlice.class,
        NilInputSlice.class,
        SegmentsInputSlice.class,
        StageInputSlice.class,

        // Other
        PassthroughAggregatorFactory.class,
        NilInputSource.class
    );

    FAULT_CLASSES.forEach(module::registerSubtypes);
    module.addSerializer(new CounterSnapshotsSerializer());
    return Collections.singletonList(module);
  }

  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  @LazySingleton
  public Bouncer makeBouncer(final DruidProcessingConfig processingConfig, @Self Set<NodeRole> nodeRoles)
  {
    if (nodeRoles.contains(NodeRole.PEON) && !nodeRoles.contains(NodeRole.INDEXER)) {
      // CliPeon -> use only one thread regardless of configured # of processing threads. This matches the expected
      // resource usage pattern for CliPeon-based tasks (one task / one working thread per JVM).
      return new Bouncer(1);
    } else {
      return new Bouncer(processingConfig.getNumThreads());
    }
  }
}
