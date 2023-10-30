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
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.StringUtils;

@JsonTypeName("sampled_table")
public class SampledTableDataSource extends TableDataSource
{
  private final SamplingType samplingType;
  private final int samplingPercentage;

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
      @JsonProperty("samplingPercentage") int samplingPercentage
  )
  {
    super(name);
    this.samplingType = samplingType;
    this.samplingPercentage = samplingPercentage;
  }

  @JsonCreator
  public static SampledTableDataSource create(
      @JsonProperty("name")final String name,
      @JsonProperty("samplingType")final String samplingType,
      @JsonProperty("samplingPercentage")final int samplingPercentage)
  {
    return new SampledTableDataSource(name, SamplingType.fromString(samplingType), samplingPercentage);
  }


  @JsonProperty
  public SamplingType getSamplingType()
  {
    return samplingType;
  }

  @JsonProperty
  public float getSamplingPercentage()
  {
    return samplingPercentage;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SampledTableDataSource)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    SampledTableDataSource that = (SampledTableDataSource) o;

    if (samplingPercentage != that.samplingPercentage) {
      return false;
    }
    return samplingType == that.samplingType;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (samplingType != null ? samplingType.hashCode() : 0);
    result = 31 * result + samplingPercentage;
    return result;
  }
}
