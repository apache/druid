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

package org.apache.druid.indexing.materializedview;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.HadoopTuningConfig;
import org.apache.druid.indexer.hadoop.DatasourceIngestionSpec;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializedViewSupervisorSpec implements SupervisorSpec
{
  private static final String TASK_PREFIX = "index_materialized_view";
  private static final String SUPERVISOR_TYPE = "materialized_view";
  private final String baseDataSource;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] aggregators;
  private final HadoopTuningConfig tuningConfig;
  private final String dataSourceName;
  private final String hadoopCoordinates;
  private final List<String> hadoopDependencyCoordinates;
  private final String classpathPrefix;
  private final Map<String, Object> context;
  private final Set<String> metrics;
  private final Set<String> dimensions;
  private final ObjectMapper objectMapper;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final ChatHandlerProvider chatHandlerProvider;
  private final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final boolean suspended;

  public MaterializedViewSupervisorSpec(
      @JsonProperty("baseDataSource") String baseDataSource,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("tuningConfig") HadoopTuningConfig tuningConfig,
      @JsonProperty("dataSource") String dataSourceName,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager,
      @JacksonInject SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      @JacksonInject IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      @JacksonInject MaterializedViewTaskConfig config,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(baseDataSource),
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    this.baseDataSource = baseDataSource;

    this.dimensionsSpec = Preconditions.checkNotNull(
        dimensionsSpec,
        "dimensionsSpec cannot be null. Please provide a dimensionsSpec"
    );
    this.aggregators = Preconditions.checkNotNull(
        aggregators,
        "metricsSpec cannot be null. Please provide a metricsSpec"
    );
    this.tuningConfig = Preconditions.checkNotNull(
        tuningConfig,
        "tuningConfig cannot be null. Please provide tuningConfig"
    );

    this.dataSourceName = dataSourceName == null
                          ? StringUtils.format(
        "%s-%s",
        baseDataSource,
        DigestUtils.sha1Hex(dimensionsSpec.toString()).substring(0, 8)
    )
                          : dataSourceName;
    this.hadoopCoordinates = hadoopCoordinates;
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    this.classpathPrefix = classpathPrefix;
    this.context = context == null ? new HashMap<>() : context;
    this.objectMapper = objectMapper;
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.config = config;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
    this.suspended = suspended != null ? suspended : false;

    this.metrics = new HashSet<>();
    for (AggregatorFactory aggregatorFactory : aggregators) {
      metrics.add(aggregatorFactory.getName());
    }
    this.dimensions = new HashSet<>();
    for (DimensionSchema schema : dimensionsSpec.getDimensions()) {
      dimensions.add(schema.getName());
    }
  }

  public HadoopIndexTask createTask(Interval interval, String version, List<DataSegment> segments)
  {
    String taskId = StringUtils.format("%s_%s_%s", TASK_PREFIX, dataSourceName, DateTimes.nowUtc());

    // generate parser
    Map<String, Object> parseSpec = new HashMap<>();
    parseSpec.put("format", "timeAndDims");
    parseSpec.put("dimensionsSpec", dimensionsSpec);
    Map<String, Object> parser = new HashMap<>();
    parser.put("type", "map");
    parser.put("parseSpec", parseSpec);

    //generate HadoopTuningConfig
    HadoopTuningConfig tuningConfigForTask = new HadoopTuningConfig(
        tuningConfig.getWorkingPath(),
        version,
        tuningConfig.getPartitionsSpec(),
        tuningConfig.getShardSpecs(),
        tuningConfig.getIndexSpec(),
        tuningConfig.getIndexSpecForIntermediatePersists(),
        tuningConfig.getRowFlushBoundary(),
        tuningConfig.getMaxBytesInMemory(),
        tuningConfig.isLeaveIntermediate(),
        tuningConfig.isCleanupOnFailure(),
        tuningConfig.isOverwriteFiles(),
        tuningConfig.isIgnoreInvalidRows(),
        tuningConfig.getJobProperties(),
        tuningConfig.isCombineText(),
        tuningConfig.getUseCombiner(),
        tuningConfig.getRowFlushBoundary(),
        tuningConfig.getBuildV9Directly(),
        tuningConfig.getNumBackgroundPersistThreads(),
        tuningConfig.isForceExtendableShardSpecs(),
        true,
        tuningConfig.getUserAllowedHadoopPrefix(),
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.isUseYarnRMJobStatusFallback()
    );

    // generate granularity
    ArbitraryGranularitySpec granularitySpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        ImmutableList.of(interval)
    );

    // generate DataSchema
    DataSchema dataSchema = new DataSchema(
        dataSourceName,
        parser,
        aggregators,
        granularitySpec,
        TransformSpec.NONE,
        objectMapper
    );

    // generate DatasourceIngestionSpec
    DatasourceIngestionSpec datasourceIngestionSpec = new DatasourceIngestionSpec(
        baseDataSource,
        null,
        ImmutableList.of(interval),
        segments,
        null,
        null,
        null,
        false,
        null
    );

    // generate HadoopIOConfig
    Map<String, Object> inputSpec = new HashMap<>();
    inputSpec.put("type", "dataSource");
    inputSpec.put("ingestionSpec", datasourceIngestionSpec);
    HadoopIOConfig hadoopIOConfig = new HadoopIOConfig(inputSpec, null, null);

    // generate HadoopIngestionSpec
    HadoopIngestionSpec spec = new HadoopIngestionSpec(dataSchema, hadoopIOConfig, tuningConfigForTask);

    // generate HadoopIndexTask
    HadoopIndexTask task = new HadoopIndexTask(
        taskId,
        spec,
        hadoopCoordinates,
        hadoopDependencyCoordinates,
        classpathPrefix,
        objectMapper,
        context,
        authorizerMapper,
        chatHandlerProvider
    );

    return task;
  }

  public Set<String> getDimensions()
  {
    return dimensions;
  }

  public Set<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty("baseDataSource")
  public String getBaseDataSource()
  {
    return baseDataSource;
  }

  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getMetricsSpec()
  {
    return aggregators;
  }

  @JsonProperty("tuningConfig")
  public HadoopTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("dataSource")
  public String getDataSourceName()
  {
    return dataSourceName;
  }

  @JsonProperty("hadoopCoordinates")
  public String getHadoopCoordinates()
  {
    return hadoopCoordinates;
  }

  @JsonProperty("hadoopDependencyCoordinates")
  public List<String> getSadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty("classpathPrefix")
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @JsonProperty("context")
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return SUPERVISOR_TYPE;
  }

  @Override
  @JsonProperty("source")
  public String getSource()
  {
    return getBaseDataSource();
  }

  @Override
  public String getId()
  {
    return StringUtils.format("MaterializedViewSupervisor-%s", dataSourceName);
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new MaterializedViewSupervisor(
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        this
    );
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(dataSourceName);
  }

  @Override
  public SupervisorSpec createSuspendedSpec()
  {
    return new MaterializedViewSupervisorSpec(
        baseDataSource,
        dimensionsSpec,
        aggregators,
        tuningConfig,
        dataSourceName,
        hadoopCoordinates,
        hadoopDependencyCoordinates,
        classpathPrefix,
        context,
        true,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        authorizerMapper,
        chatHandlerProvider,
        supervisorStateManagerConfig
    );
  }

  @Override
  public SupervisorSpec createRunningSpec()
  {
    return new MaterializedViewSupervisorSpec(
        baseDataSource,
        dimensionsSpec,
        aggregators,
        tuningConfig,
        dataSourceName,
        hadoopCoordinates,
        hadoopDependencyCoordinates,
        classpathPrefix,
        context,
        false,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        authorizerMapper,
        chatHandlerProvider,
        supervisorStateManagerConfig
    );
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig()
  {
    return supervisorStateManagerConfig;
  }

  @Override
  public String toString()
  {
    return "MaterializedViewSupervisorSpec{" +
           "baseDataSource=" + baseDataSource +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           '}';
  }
}
