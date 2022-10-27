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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.UnnestSegmentReference;
import org.apache.druid.utils.JvmUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class UnnestDataSource implements DataSource
{
  private final DataSource base;
  private final String column;
  private final boolean dedup;
  private final String outputName;


  private UnnestDataSource(
      DataSource dataSource,
      String columnName,
      String outputName,
      boolean dedup
  )
  {
    this.base = dataSource;
    this.column = columnName;
    this.dedup = dedup;
    this.outputName = outputName;
  }

  /**
   * Create an unnest dataSource from a string condition.
   */
  @JsonCreator
  public static UnnestDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("column") String columnName,
      @JsonProperty("dedup") boolean dedup,
      @JsonProperty("outputName") String outputName
  )
  {
    return new UnnestDataSource(base, columnName, outputName, dedup);
  }

  @JsonProperty("base")
  public DataSource getBase()
  {
    return base;
  }

  @JsonProperty("column")
  public String getColumn()
  {
    return column;
  }

  @JsonProperty("dedup")
  public boolean getDedup()
  {
    return dedup;
  }

  @JsonProperty("outputName")
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public Set<String> getTableNames()
  {
    return base.getTableNames();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(base);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 1) {
      throw new IAE("Expected [1] child, got [%d]", children.size());
    }
    return new UnnestDataSource(children.get(0), column, outputName, dedup);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return base.isCacheable(isBroker);
  }

  @Override
  public boolean isGlobal()
  {
    return base.isGlobal();
  }

  @Override
  public boolean isConcrete()
  {
    return base.isConcrete();
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTimeAccumulator
  )
  {
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> {
          if (column == null) {
            return Function.identity();
          } else if (column.isEmpty()) {
            return Function.identity();
          } else {
            return baseSegment ->
                new UnnestSegmentReference(
                    baseSegment,
                    column,
                    outputName,
                    dedup
                );
          }
        }
    );

  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
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
    UnnestDataSource that = (UnnestDataSource) o;
    return column.equals(that.column)
           && outputName.equals(that.outputName)
           && dedup == that.dedup
           && base.equals(that.base);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, column, dedup, outputName);
  }
}


