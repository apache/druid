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

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The data source for representing an unnest operation.
 *
 * An unnest data source has the following:
 * a base data source which is to be unnested
 * the column name of the MVD which will be unnested
 * the name of the column that will hold the unnested values
 * and an allowlist serving as a filter of which values in the MVD will be unnested.
 */
public class UnnestDataSource implements DataSource
{
  private final DataSource base;
  private final String column;
  private final String outputName;
  private final LinkedHashSet<String> allowList;

  private UnnestDataSource(
      DataSource dataSource,
      String columnName,
      String outputName,
      LinkedHashSet<String> allowList
  )
  {
    this.base = dataSource;
    this.column = columnName;
    this.outputName = outputName;
    this.allowList = allowList;
  }

  @JsonCreator
  public static UnnestDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("column") String columnName,
      @JsonProperty("outputName") String outputName,
      @Nullable @JsonProperty("allowList") LinkedHashSet<String> allowList
  )
  {
    return new UnnestDataSource(base, columnName, outputName, allowList);
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

  @JsonProperty("outputName")
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty("allowList")
  public LinkedHashSet<String> getAllowList()
  {
    return allowList;
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
    return new UnnestDataSource(children.get(0), column, outputName, allowList);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
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
    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(
        query,
        cpuTimeAccumulator
    );
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> {
          if (column == null) {
            return segmentMapFn;
          } else if (column.isEmpty()) {
            return segmentMapFn;
          } else {
            return
                baseSegment ->
                    new UnnestSegmentReference(
                        segmentMapFn.apply(baseSegment),
                        column,
                        outputName,
                        allowList
                    );
          }
        }
    );

  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new UnnestDataSource(newSource, column, outputName, allowList);
  }

  @Override
  public byte[] getCacheKey()
  {
    // The column being unnested would need to be part of the cache key
    // as the results are dependent on what column is being unnested.
    // Currently, it is not cacheable.
    // Future development should use the table name and column came to
    // create an appropriate cac
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
           && base.equals(that.base);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, column, outputName);
  }
}


