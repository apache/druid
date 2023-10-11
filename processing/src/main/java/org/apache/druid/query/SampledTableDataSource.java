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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.SegmentReference;

@JsonTypeName("sampled_table")
public class SampledTableDataSource implements DataSource
{
  private final String name;
  private final SamplingType samplingType;
  private final float samplingPercentage;
  public enum SamplingType implements Cacheable
  {
    FIXED_SHARD;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static SamplingType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[] {(byte) this.ordinal()};
    }
  }

  @JsonCreator
  public SampledTableDataSource(
      @JsonProperty("name") String name,
      @JsonProperty("samplingType") SamplingType samplingType,
      @JsonProperty("samplingPercentage") float samplingPercentage
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' must be nonnull");
    this.samplingType = samplingType;
    this.samplingPercentage = samplingPercentage;
  }

  @JsonCreator
public static SampledTableDataSource create(final String name, final String samplingType, final float samplingRatio)
  {
    return new SampledTableDataSource(name, SamplingType.fromString(samplingType), samplingRatio);
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.singleton(name);
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return true;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTime
  )
  {
    return Function.identity();
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return newSource;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public DataSourceAnalysis getAnalysis()
  {
    return new DataSourceAnalysis(this, null, null, Collections.emptyList());
  }

  public SamplingType getSamplingType() {
    return samplingType;
  }

  public float getSamplingPercentage() {
    return samplingPercentage;
  }

  @Override
  public String toString()
  {
    return name;
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
    SampledTableDataSource that = (SampledTableDataSource) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name);
  }
}
