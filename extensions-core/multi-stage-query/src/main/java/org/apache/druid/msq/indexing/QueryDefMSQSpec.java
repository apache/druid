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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Objects;

public class QueryDefMSQSpec extends MSQSpec
{
  protected final QueryDefinition queryDef;

  // jackson defaults
  public QueryDefMSQSpec()
  {
    super();
    queryDef = null;
  }

  @JsonCreator
  public QueryDefMSQSpec(
      @JsonProperty("queryDef") QueryDefinition queryDef,
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig)
  {
    super(columnMappings, destination, assignmentStrategy, tuningConfig);
    this.queryDef = Preconditions.checkNotNull(queryDef, "queryDef");
  }

  @JsonProperty("queryDef")
  public QueryDefinition getQueryDef()
  {
    return queryDef;
  }

  @Override
  public String getId()
  {
    return getContext().getValue(QueryContexts.QUERY_ID);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    QueryDefMSQSpec that = (QueryDefMSQSpec) o;
    return Objects.equals(queryDef, that.queryDef);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), queryDef);
  }

  public static class Builder
  {
    private ColumnMappings columnMappings;
    private MSQDestination destination = TaskReportMSQDestination.instance();
    private WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    private MSQTuningConfig tuningConfig;
    private QueryDefinition queryDef;

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

    public QueryDefMSQSpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }
      return new QueryDefMSQSpec(
          queryDef,
          columnMappings,
          destination,
          assignmentStrategy,
          tuningConfig
      );
    }
  }

  @Override
  public QueryContext getContext()
  {
    return queryDef.getContext();
  }
}
