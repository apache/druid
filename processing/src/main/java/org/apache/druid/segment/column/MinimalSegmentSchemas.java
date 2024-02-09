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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class MinimalSegmentSchemas
{
  private final Map<String, SegmentStats> segmentStatsMap;
  private final Map<String, SchemaPayload> schemaPayloadMap;

  @JsonCreator
  public MinimalSegmentSchemas(
      @JsonProperty("segmentStatsMap") Map<String, SegmentStats> segmentStatsMap,
      @JsonProperty("schemaPayloadMap") Map<String, SchemaPayload> schemaPayloadMap)
  {
    this.segmentStatsMap = segmentStatsMap;
    this.schemaPayloadMap = schemaPayloadMap;
  }

  public MinimalSegmentSchemas()
  {
    this.segmentStatsMap = new HashMap<>();
    this.schemaPayloadMap = new HashMap<>();
  }

  @JsonProperty
  public Map<String, SegmentStats> getSegmentStatsMap()
  {
    return segmentStatsMap;
  }

  @JsonProperty
  public Map<String, SchemaPayload> getSchemaPayloadMap()
  {
    return schemaPayloadMap;
  }

  public void addSchema(String segmentId, String fingerprint, long numRows, SchemaPayload schemaPayload)
  {
    segmentStatsMap.put(segmentId, new SegmentStats(numRows, fingerprint));
    schemaPayloadMap.put(fingerprint, schemaPayload);
  }

  public boolean isNonEmpty()
  {
    return segmentStatsMap.size() > 0;
  }

  public void add(MinimalSegmentSchemas other)
  {
    this.segmentStatsMap.putAll(other.getSegmentStatsMap());
    this.schemaPayloadMap.putAll(other.getSchemaPayloadMap());
  }

  public static class SegmentStats
  {
    Long numRows;
    String fingerprint;

    @JsonCreator
    public SegmentStats(
        @JsonProperty("numRows") Long numRows,
        @JsonProperty("fingerprint") String fingerprint)
    {
      this.numRows = numRows;
      this.fingerprint = fingerprint;
    }

    @JsonProperty
    public long getNumRows()
    {
      return numRows;
    }

    @JsonProperty
    public String getFingerprint()
    {
      return fingerprint;
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
      SegmentStats that = (SegmentStats) o;
      return Objects.equals(numRows, that.numRows) && Objects.equals(fingerprint, that.fingerprint);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(numRows, fingerprint);
    }

    @Override
    public String toString()
    {
      return "SegmentStats{" +
             "numRows=" + numRows +
             ", fingerprint='" + fingerprint + '\'' +
             '}';
    }
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
    MinimalSegmentSchemas that = (MinimalSegmentSchemas) o;
    return Objects.equals(segmentStatsMap, that.segmentStatsMap) && Objects.equals(
        schemaPayloadMap,
        that.schemaPayloadMap
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentStatsMap, schemaPayloadMap);
  }

  @Override
  public String toString()
  {
    return "MinimalSegmentSchemas{" +
           "segmentStatsMap=" + segmentStatsMap +
           ", schemaPayloadMap=" + schemaPayloadMap +
           '}';
  }
}
