/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.materializedview;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.timeline.DataSegment;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializedViewSupervisorSpec implements SupervisorSpec 
{
  private static final String TASK_PREFIX = "index_materialized_view";
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
  private final SQLMetadataSegmentManager segmentManager;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  
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
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager,
      @JacksonInject SQLMetadataSegmentManager segmentManager,
      @JacksonInject IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      @JacksonInject MaterializedViewTaskConfig config
  )
  {
    this.baseDataSource = Preconditions.checkNotNull(baseDataSource, "baseDataSource cannot be null. Please provide a baseDataSource.");
    this.dimensionsSpec = Preconditions.checkNotNull(dimensionsSpec, "dimensionsSpec cannot be null. Please provide a dimensionsSpec");
    this.aggregators = Preconditions.checkNotNull(aggregators, "metricsSpec cannot be null. Please provide a metricsSpec");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig cannot be null. Please provide tuningConfig");
    
    this.dataSourceName = dataSourceName == null ? 
        StringUtils.format("%s-%s", baseDataSource, DigestUtils.sha1Hex(dimensionsSpec.toString()).substring(0, 8)) :
        dataSourceName;
    this.hadoopCoordinates = hadoopCoordinates;
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    this.classpathPrefix = classpathPrefix;
    this.context = context == null ? Maps.newHashMap() : context;
    this.objectMapper = objectMapper;
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.segmentManager = segmentManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.config = config;
    
    this.metrics = Sets.newHashSet();
    for (AggregatorFactory aggregatorFactory : aggregators) {
      metrics.add(aggregatorFactory.getName());
    }
    this.dimensions = Sets.newHashSet();
    for (DimensionSchema schema : dimensionsSpec.getDimensions()) {
      dimensions.add(schema.getName());
    }
  }
  
  public HadoopIndexTask createTask(Interval interval, String version, List<DataSegment> segments)
  {
    String taskId = StringUtils.format("%s_%s_%s", TASK_PREFIX, dataSourceName, DateTimes.nowUtc());
    
    // generate parser
    Map<String, Object> parseSpec = Maps.newHashMap();
    parseSpec.put("format", "timeAndDims");
    parseSpec.put("dimensionsSpec", dimensionsSpec);
    Map<String, Object> parser = Maps.newHashMap();
    parser.put("type", "map");
    parser.put("parseSpec", parseSpec);
    
    //generate HadoopTuningConfig
    Map<String, Object> tmp = Maps.newHashMap();
    tmp.putAll(objectMapper.convertValue(tuningConfig, Map.class));
    tmp.put("version", version);
    tmp.put("type", "hadoop");
    tmp.put("useExplicitVersion", true);
    HadoopTuningConfig tuningConfigForTask = objectMapper.convertValue(tmp, HadoopTuningConfig.class);
    
    // generate granularity
    tmp.clear();
    tmp.put("type", "arbitrary");
    tmp.put("intervals", ImmutableList.of(interval));
    ArbitraryGranularitySpec granularitySpec = objectMapper.convertValue(tmp, ArbitraryGranularitySpec.class);
    tmp.clear();

    // generate DataSchema
    tmp.clear();
    tmp.put("dataSource", dataSourceName);
    tmp.put("parser", parser);
    tmp.put("metricsSpec", aggregators);
    DataSchema dataSchema = objectMapper.convertValue(tmp, DataSchema.class).withGranularitySpec(granularitySpec);
    
    // generate DatasourceIngestionSpec
    tmp.clear();
    tmp.put("dataSource", baseDataSource);
    tmp.put("intervals", ImmutableList.of(interval));
    tmp.put("segments", segments);
    DatasourceIngestionSpec datasourceIngestionSpec = objectMapper.convertValue(tmp, DatasourceIngestionSpec.class);

    // generate HadoopIOConfig
    tmp.clear();
    Map<String, Object> inputSpec = Maps.newHashMap();
    inputSpec.put("type", "dataSource");
    inputSpec.put("ingestionSpec", datasourceIngestionSpec);
    tmp.put("type", "hadoop");
    tmp.put("inputSpec", inputSpec);
    HadoopIOConfig hadoopIOConfig = objectMapper.convertValue(tmp, HadoopIOConfig.class);
    
    // generate HadoopIngestionSpec
    HadoopIngestionSpec spec = new HadoopIngestionSpec(dataSchema, hadoopIOConfig, tuningConfigForTask);
    
    // generate HadoopIndexTask
    tmp.clear();
    tmp.put("type", "index_hadoop");
    tmp.put("id", taskId);
    tmp.put("spec", spec);
    tmp.put("hadoopCoordinates", hadoopCoordinates);
    tmp.put("hadoopDependencyCoordinates", hadoopDependencyCoordinates);
    tmp.put("classpathPrefix", classpathPrefix);
    tmp.put("context", context);
    HadoopIndexTask task = objectMapper.convertValue(tmp, HadoopIndexTask.class);

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
        segmentManager,
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
  public String toString()
  {
    return "MaterializedViewSupervisorSpec{" +
        "baseDataSource=" + baseDataSource +
        ", dimensions=" + dimensions +
        ", metrics=" + metrics +
        '}';
  }
}
