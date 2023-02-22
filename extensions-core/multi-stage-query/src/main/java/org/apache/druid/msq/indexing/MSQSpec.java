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
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.Query;

import javax.annotation.Nullable;
import java.util.Objects;

public class MSQSpec
{
  private final Query<?> query;
  private final ColumnMappings columnMappings;
  private final MSQDestination destination;
  private final WorkerAssignmentStrategy assignmentStrategy;
  private final MSQTuningConfig tuningConfig;

  @JsonCreator
  public MSQSpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") @Nullable ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.destination = Preconditions.checkNotNull(destination, "destination");
    this.assignmentStrategy = Preconditions.checkNotNull(assignmentStrategy, "assignmentStrategy");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
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
    return Objects.equals(query, that.query)
           && Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(destination, that.destination)
           && Objects.equals(assignmentStrategy, that.assignmentStrategy)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, columnMappings, destination, assignmentStrategy, tuningConfig);
  }

  public static class Builder
  {
    private Query<?> query;
    private ColumnMappings columnMappings;
    private MSQDestination destination = TaskReportMSQDestination.instance();
    private WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    private MSQTuningConfig tuningConfig;

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

    public MSQSpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }

      return new MSQSpec(query, columnMappings, destination, assignmentStrategy, tuningConfig);
    }
  }
}
