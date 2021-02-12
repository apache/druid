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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SurrogateTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.partition.HashPartitioner;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PartialDimensionCardinalityTask extends PerfectRollupWorkerTask
{
  public static final String TYPE = "partial_dimension_cardinality";

  private final int numAttempts;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;

  private final ObjectMapper jsonMapper;

  @JsonCreator
  PartialDimensionCardinalityTask(
      // id shouldn't be null except when this task is created by ParallelIndexSupervisorTask
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        ingestionSchema.getDataSchema(),
        ingestionSchema.getTuningConfig(),
        context
    );

    Preconditions.checkArgument(
        ingestionSchema.getTuningConfig().getPartitionsSpec() instanceof HashedPartitionsSpec,
        "%s partitionsSpec required",
        HashedPartitionsSpec.NAME
    );

    this.numAttempts = numAttempts;
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.jsonMapper = jsonMapper;
  }

  @JsonProperty
  private int getNumAttempts()
  {
    return numAttempts;
  }

  @JsonProperty("spec")
  private ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty
  private String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    if (!getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals().isEmpty()) {
      return tryTimeChunkLock(
          new SurrogateTaskActionClient(supervisorTaskId, taskActionClient),
          getIngestionSchema().getDataSchema().getGranularitySpec().inputIntervals()
      );
    } else {
      return true;
    }
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    DataSchema dataSchema = ingestionSchema.getDataSchema();
    GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();

    HashedPartitionsSpec partitionsSpec = (HashedPartitionsSpec) tuningConfig.getPartitionsSpec();
    Preconditions.checkNotNull(partitionsSpec, "partitionsSpec required in tuningConfig");

    InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );
    InputFormat inputFormat = inputSource.needsFormat()
                              ? ParallelIndexSupervisorTask.getInputFormat(ingestionSchema)
                              : null;
    final RowIngestionMeters buildSegmentsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        buildSegmentsMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final boolean determineIntervals = granularitySpec.inputIntervals().isEmpty();

    try (
        final CloseableIterator<InputRow> inputRowIterator = AbstractBatchIndexTask.inputSourceReader(
            toolbox.getIndexingTmpDir(),
            dataSchema,
            inputSource,
            inputFormat,
            determineIntervals ? Objects::nonNull : AbstractBatchIndexTask.defaultRowFilter(granularitySpec),
            buildSegmentsMeters,
            parseExceptionHandler
        );
    ) {
      Map<Interval, byte[]> cardinalities = determineCardinalities(
          inputRowIterator,
          granularitySpec
      );

      sendReport(
          toolbox,
          new DimensionCardinalityReport(getId(), cardinalities)
      );
    }

    return TaskStatus.success(getId());
  }

  private Map<Interval, byte[]> determineCardinalities(
      CloseableIterator<InputRow> inputRowIterator,
      GranularitySpec granularitySpec
  )
  {
    Map<Interval, HllSketch> intervalToCardinalities = new HashMap<>();
    while (inputRowIterator.hasNext()) {
      InputRow inputRow = inputRowIterator.next();
      // null rows are filtered out by FilteringCloseableInputRowIterator
      DateTime timestamp = inputRow.getTimestamp();
      final Interval interval;
      if (granularitySpec.inputIntervals().isEmpty()) {
        interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
      } else {
        final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
        // this interval must exist since it passed the rowFilter
        assert optInterval.isPresent();
        interval = optInterval.get();
      }
      Granularity queryGranularity = granularitySpec.getQueryGranularity();

      HllSketch hllSketch = intervalToCardinalities.computeIfAbsent(
          interval,
          (intervalKey) -> DimensionCardinalityReport.createHllSketchForReport()
      );
      // For cardinality estimation, we want to consider unique rows instead of unique hash buckets and therefore
      // we do not use partition dimensions in computing the group key
      List<Object> groupKey = HashPartitioner.extractKeys(
          Collections.emptyList(),
          queryGranularity.bucketStart(timestamp).getMillis(),
          inputRow
      );

      try {
        hllSketch.update(
            jsonMapper.writeValueAsBytes(groupKey)
        );
      }
      catch (JsonProcessingException jpe) {
        throw new RuntimeException(jpe);
      }
    }

    // Serialize the collectors for sending to the supervisor task
    Map<Interval, byte[]> newMap = new HashMap<>();
    for (Map.Entry<Interval, HllSketch> entry : intervalToCardinalities.entrySet()) {
      newMap.put(entry.getKey(), entry.getValue().toCompactByteArray());
    }
    return newMap;
  }

  private void sendReport(TaskToolbox toolbox, DimensionCardinalityReport report)
  {
    final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientFactory().build(
        new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
        getId(),
        1, // always use a single http thread
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );
    taskClient.report(supervisorTaskId, report);
  }

}
