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

package org.apache.druid.server.coordinator.config;

import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskTransformConfig;
import org.joda.time.Period;

import java.util.Map;

public class DataSourceCompactionConfigBuilder
{
  private String dataSource;
  private Integer taskPriority;
  private Long inputSegmentSizeBytes;
  private Period skipOffsetFromLatest;
  private UserCompactionTaskQueryTuningConfig tuningConfig;
  private UserCompactionTaskGranularityConfig granularitySpec;
  private UserCompactionTaskDimensionsConfig dimensionsSpec;
  private AggregatorFactory[] metricsSpec;
  private UserCompactionTaskTransformConfig transformSpec;
  private UserCompactionTaskIOConfig ioConfig;
  private CompactionEngine engine;
  private Map<String, Object> taskContext;

  public DataSourceCompactionConfig build()
  {
    return new DataSourceCompactionConfig(
        dataSource,
        taskPriority,
        inputSegmentSizeBytes,
        skipOffsetFromLatest,
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        ioConfig,
        engine,
        taskContext
    );
  }

  public DataSourceCompactionConfigBuilder forDataSource(String dataSource)
  {
    this.dataSource = dataSource;
    return this;
  }

  public DataSourceCompactionConfigBuilder withTaskPriority(Integer taskPriority)
  {
    this.taskPriority = taskPriority;
    return this;
  }

  public DataSourceCompactionConfigBuilder withInputSegmentSizeBytes(Long inputSegmentSizeBytes)
  {
    this.inputSegmentSizeBytes = inputSegmentSizeBytes;
    return this;
  }

  public DataSourceCompactionConfigBuilder withSkipOffsetFromLatest(Period skipOffsetFromLatest)
  {
    this.skipOffsetFromLatest = skipOffsetFromLatest;
    return this;
  }

  public DataSourceCompactionConfigBuilder withTuningConfig(
      UserCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    this.tuningConfig = tuningConfig;
    return this;
  }

  public DataSourceCompactionConfigBuilder withGranularitySpec(
      UserCompactionTaskGranularityConfig granularitySpec
  )
  {
    this.granularitySpec = granularitySpec;
    return this;
  }

  public DataSourceCompactionConfigBuilder withDimensionsSpec(
      UserCompactionTaskDimensionsConfig dimensionsSpec
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    return this;
  }

  public DataSourceCompactionConfigBuilder withMetricsSpec(AggregatorFactory[] metricsSpec)
  {
    this.metricsSpec = metricsSpec;
    return this;
  }

  public DataSourceCompactionConfigBuilder withTransformSpec(
      UserCompactionTaskTransformConfig transformSpec
  )
  {
    this.transformSpec = transformSpec;
    return this;
  }

  public DataSourceCompactionConfigBuilder withIoConfig(UserCompactionTaskIOConfig ioConfig)
  {
    this.ioConfig = ioConfig;
    return this;
  }

  public DataSourceCompactionConfigBuilder withEngine(CompactionEngine engine)
  {
    this.engine = engine;
    return this;
  }

  public DataSourceCompactionConfigBuilder withTaskContext(Map<String, Object> taskContext)
  {
    this.taskContext = taskContext;
    return this;
  }
}
