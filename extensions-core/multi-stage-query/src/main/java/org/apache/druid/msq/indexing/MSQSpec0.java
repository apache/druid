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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.indexing.MSQSpec.Builder;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Collections;
import java.util.List;

public abstract class MSQSpec0
{
  protected final ColumnMappings columnMappings;
  protected final MSQDestination destination;
  protected final WorkerAssignmentStrategy assignmentStrategy;
  protected final MSQTuningConfig tuningConfig;
  protected final List<AggregatorFactory> compactionMetricSpec;
  protected final QueryContext queryContext;
  protected final QueryDefinition queryDef;

  public MSQSpec0()
  {
    columnMappings = null;
    destination = null;
    assignmentStrategy = null;
    tuningConfig = null;
    compactionMetricSpec = Collections.emptyList();
    queryContext = QueryContext.empty();
    queryDef = null;
  }

  @JsonCreator
  public MSQSpec0(
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig,
      @JsonProperty("compactionMetricSpec") List<AggregatorFactory> compactionMetricSpec1,
      @JsonProperty("queryContext") QueryContext queryContext,
      @JsonProperty("queryDef") QueryDefinition queryDef
  )
  {
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.destination = Preconditions.checkNotNull(destination, "destination");
    this.assignmentStrategy = Preconditions.checkNotNull(assignmentStrategy, "assignmentStrategy");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.compactionMetricSpec = compactionMetricSpec1;
    this.queryContext = queryContext == null ? QueryContext.empty() : queryContext;
    this.queryDef = queryDef;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonProperty("queryContext")
  @JsonInclude(value = Include.NON_DEFAULT)
  public QueryContext getContext()
  {
    return queryContext;
  }

  @JsonProperty("columnMappings")
  public ColumnMappings getColumnMappings()
  {
    return columnMappings;
  }

  @JsonProperty
  public MSQDestination getDestination()
  {
    return destination;
  }

  @JsonProperty
  public WorkerAssignmentStrategy getAssignmentStrategy()
  {
    return assignmentStrategy;
  }

  @JsonProperty
  public MSQTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("compactionMetricSpec")
  public List<AggregatorFactory> getCompactionMetricSpec()
  {
    return compactionMetricSpec;
  }

  public String getId()
  {
    return getContext().getString(BaseQuery.QUERY_ID);
  }

  @JsonProperty("queryDef")
  @JsonInclude(value = Include.NON_NULL)
  public QueryDefinition getQueryDef()
  {
    return queryDef;
  }
}
