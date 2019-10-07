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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SamplerResponse
{
  private final String cacheKey;
  private final Integer numRowsRead;
  private final Integer numRowsIndexed;
  private final List<SamplerResponseRow> data;

  public SamplerResponse(String cacheKey, Integer numRowsRead, Integer numRowsIndexed, List<SamplerResponseRow> data)
  {
    this.cacheKey = cacheKey;
    this.numRowsRead = numRowsRead;
    this.numRowsIndexed = numRowsIndexed;
    this.data = data;
  }

  @JsonProperty
  public String getCacheKey()
  {
    return cacheKey;
  }

  @JsonProperty
  public Integer getNumRowsRead()
  {
    return numRowsRead;
  }

  @JsonProperty
  public Integer getNumRowsIndexed()
  {
    return numRowsIndexed;
  }

  @JsonProperty
  public List<SamplerResponseRow> getData()
  {
    return data;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class SamplerResponseRow
  {
    private final String raw;
    private final Map<String, Object> parsed;
    private final Boolean unparseable;
    private final String error;

    public SamplerResponseRow(
        String raw,
        Map<String, Object> parsed,
        Boolean unparseable,
        String error
    )
    {
      this.raw = raw;
      this.parsed = parsed;
      this.unparseable = unparseable;
      this.error = error;
    }

    @JsonProperty
    public String getRaw()
    {
      return raw;
    }

    @JsonProperty
    public Map<String, Object> getParsed()
    {
      return parsed;
    }

    @JsonProperty
    public Boolean isUnparseable()
    {
      return unparseable;
    }

    @JsonProperty
    public String getError()
    {
      return error;
    }

    public SamplerResponseRow withParsed(Map<String, Object> parsed)
    {
      return new SamplerResponseRow(raw, parsed, unparseable, error);
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
      SamplerResponseRow that = (SamplerResponseRow) o;
      return Objects.equals(raw, that.raw) &&
             Objects.equals(parsed, that.parsed) &&
             Objects.equals(unparseable, that.unparseable) &&
             Objects.equals(error, that.error);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(raw, parsed, unparseable, error);
    }

    @Override
    public String toString()
    {
      return "SamplerResponseRow{" +
             "raw='" + raw + '\'' +
             ", parsed=" + parsed +
             ", unparseable=" + unparseable +
             ", error='" + error + '\'' +
             '}';
    }
  }
}
