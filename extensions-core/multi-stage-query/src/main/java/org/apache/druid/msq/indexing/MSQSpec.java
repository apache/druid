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
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MSQSpec
{
  protected final ColumnMappings columnMappings;
  protected final MSQDestination destination;
  protected final WorkerAssignmentStrategy assignmentStrategy;
  protected final MSQTuningConfig tuningConfig;
  protected final QueryDefinition queryDef;

  public MSQSpec()
  {
    columnMappings = null;
    destination = null;
    assignmentStrategy = null;
    tuningConfig = null;
    queryDef = null;
  }

  @JsonCreator
  public MSQSpec(
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig,
      @JsonProperty("queryDef") QueryDefinition queryDef
  )
  {
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.destination = Preconditions.checkNotNull(destination, "destination");
    this.assignmentStrategy = Preconditions.checkNotNull(assignmentStrategy, "assignmentStrategy");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.queryDef = queryDef;
  }

  public QueryContext getContext()
  {
    Preconditions.checkNotNull(queryDef, "QueryDefinition should not be null");
    return queryDef.getContext();
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MSQSpec that = (MSQSpec) o;
    return Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(destination, that.destination)
           && Objects.equals(assignmentStrategy, that.assignmentStrategy)
           && Objects.equals(tuningConfig, that.tuningConfig)
           && Objects.equals(queryDef, that.queryDef);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnMappings, destination, assignmentStrategy, tuningConfig, queryDef);
  }

  public static class Builder
  {
    protected ColumnMappings columnMappings;
    protected MSQDestination destination = TaskReportMSQDestination.instance();
    protected WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    protected MSQTuningConfig tuningConfig;
    protected List<AggregatorFactory> compactionMetrics = Collections.emptyList();
    protected QueryDefinition queryDef;

    public Builder queryDef(QueryDefinition queryDef)
    {
      this.queryDef = queryDef;
      return this;
    }

    public Builder columnMappings(final ColumnMappings columnMappings)
    {
      this.columnMappings = columnMappings;
      return this;
    }

    public Builder destination(final MSQDestination destination)
    {
      this.destination = destination;
      return this;
    }

    public Builder assignmentStrategy(final WorkerAssignmentStrategy assignmentStrategy)
    {
      this.assignmentStrategy = assignmentStrategy;
      return this;
    }

    public Builder tuningConfig(final MSQTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder compactionMetrics(List<AggregatorFactory> compactionMetrics)
    {
      this.compactionMetrics=compactionMetrics;
      return this;
    }

    public MSQSpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }
      return new MSQSpec(
          columnMappings,
          destination,
          assignmentStrategy,
          tuningConfig,
          queryDef
      );
    }
  }

}
