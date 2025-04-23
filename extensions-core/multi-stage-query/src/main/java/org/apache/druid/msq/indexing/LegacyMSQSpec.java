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
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Map;
import java.util.Objects;

/**
 * Old MSQSpec with a native query in it.
 *
 * Usage of this class should be avoided in favor of {@link MSQSpec}.
 */
public class LegacyMSQSpec extends MSQSpec
{
  private final Query<?> query;

  // jackson defaults
  public LegacyMSQSpec()
  {
    super();
    this.query = null;
  }

  public LegacyMSQSpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig)
  {
    this(query, columnMappings, destination, assignmentStrategy, tuningConfig, null);
  }

  @JsonCreator
  public LegacyMSQSpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig,
      @JsonProperty("queryDef") QueryDefinition queryDef
  )
  {
    super(columnMappings, destination, assignmentStrategy, tuningConfig, queryDef);
    Preconditions.checkArgument(query == null ^ queryDef == null, "Either query or queryDef must be null!");
    this.query = query;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonProperty
  public Query<?> getQuery()
  {
    return query;
  }

  @Override
  public String getId()
  {
    return getContext().getString(BaseQuery.QUERY_ID);
  }


  @Override
  public QueryContext getContext()
  {
    if (isLegacyMode()) {
      return QueryContext.of(query.getContext());
    } else {
      return super.getContext();
    }
  }

  private boolean isLegacyMode()
  {
    return queryDef == null;
  }

  public LegacyMSQSpec withOverriddenContext(Map<String, Object> contextOverride)
  {
    Preconditions.checkArgument(queryDef == null, "queryDef must be null!");
    if (contextOverride == null || contextOverride.isEmpty()) {
      return this;
    } else {
      return new LegacyMSQSpec(
          query.withOverriddenContext(contextOverride),
          getColumnMappings(),
          getDestination(),
          getAssignmentStrategy(),
          getTuningConfig()
      );
    }
  }


  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    LegacyMSQSpec that = (LegacyMSQSpec) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), query);
  }

  public static class Builder
  {
    private Query<?> query;
    private ColumnMappings columnMappings;
    private MSQDestination destination = TaskReportMSQDestination.instance();
    private WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    private MSQTuningConfig tuningConfig;
    private QueryContext queryContext = QueryContext.empty();

    @Deprecated
    public Builder query(Query<?> query)
    {
      this.query = query;
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


    public LegacyMSQSpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }
      return new LegacyMSQSpec(
          query.withOverriddenContext(queryContext.asMap()),
          columnMappings,
          destination,
          assignmentStrategy,
          tuningConfig,
          null
      );
    }

    public Builder queryContext(QueryContext queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }
  }
}
