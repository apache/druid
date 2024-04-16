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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact representation of segment schema for multiple segments.
 */
public class MinimalSegmentSchemas
{
  // Mapping of segmentId to segment level information like schema fingerprint and numRows.
  private final Map<String, SegmentStats> segmentIdToMetadataMap;

  // Mapping of schema fingerprint to payload.
  private final Map<String, SchemaPayload> schemaFingerprintToPayloadMap;

  private final int schemaVersion;

  @JsonCreator
  public MinimalSegmentSchemas(
      @JsonProperty("segmentIdToMetadataMap") Map<String, SegmentStats> segmentIdToMetadataMap,
      @JsonProperty("schemaFingerprintToPayloadMap") Map<String, SchemaPayload> schemaFingerprintToPayloadMap,
      @JsonProperty("schemaVersion") int schemaVersion
  )
  {
    this.segmentIdToMetadataMap = segmentIdToMetadataMap;
    this.schemaFingerprintToPayloadMap = schemaFingerprintToPayloadMap;
    this.schemaVersion = schemaVersion;
  }

  public MinimalSegmentSchemas(int schemaVersion)
  {
    this.segmentIdToMetadataMap = new HashMap<>();
    this.schemaFingerprintToPayloadMap = new HashMap<>();
    this.schemaVersion = schemaVersion;
  }

  @JsonProperty
  public Map<String, SegmentStats> getSegmentIdToMetadataMap()
  {
    return segmentIdToMetadataMap;
  }

  @JsonProperty
  public Map<String, SchemaPayload> getSchemaFingerprintToPayloadMap()
  {
    return schemaFingerprintToPayloadMap;
  }

  @JsonProperty
  public int getSchemaVersion()
  {
    return schemaVersion;
  }

  public boolean isNonEmpty()
  {
    return segmentIdToMetadataMap.size() > 0;
  }

  /**
   * Add schema information for the segment.
   */
  public void addSchema(
      String segmentId,
      String fingerprint,
      long numRows,
      SchemaPayload schemaPayload
  )
  {
    segmentIdToMetadataMap.put(segmentId, new SegmentStats(numRows, fingerprint));
    schemaFingerprintToPayloadMap.put(fingerprint, schemaPayload);
  }

  /**
   * Merge with another instance.
   */
  public void merge(MinimalSegmentSchemas other)
  {
    this.segmentIdToMetadataMap.putAll(other.getSegmentIdToMetadataMap());
    this.schemaFingerprintToPayloadMap.putAll(other.getSchemaFingerprintToPayloadMap());
  }

  public int size()
  {
    return schemaFingerprintToPayloadMap.size();
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
    return Objects.equals(segmentIdToMetadataMap, that.segmentIdToMetadataMap)
           && Objects.equals(schemaFingerprintToPayloadMap, that.schemaFingerprintToPayloadMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentIdToMetadataMap, schemaFingerprintToPayloadMap);
  }

  @Override
  public String toString()
  {
    return "MinimalSegmentSchemas{" +
           "segmentIdToMetadataMap=" + segmentIdToMetadataMap +
           ", schemaFingerprintToPayloadMap=" + schemaFingerprintToPayloadMap +
           ", version='" + schemaVersion + '\'' +
           '}';
  }

  /**
   * Encapsulates segment level information like numRows, schema fingerprint.
   */
  public static class SegmentStats
  {
    final Long numRows;
    final String schemaFingerprint;

    @JsonCreator
    public SegmentStats(
        @JsonProperty("numRows") Long numRows,
        @JsonProperty("schemaFingerprint") String schemaFingerprint
    )
    {
      this.numRows = numRows;
      this.schemaFingerprint = schemaFingerprint;
    }

    @JsonProperty
    public long getNumRows()
    {
      return numRows;
    }

    @JsonProperty
    public String getSchemaFingerprint()
    {
      return schemaFingerprint;
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
      return Objects.equals(numRows, that.numRows) && Objects.equals(schemaFingerprint, that.schemaFingerprint);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(numRows, schemaFingerprint);
    }

    @Override
    public String toString()
    {
      return "SegmentStats{" +
             "numRows=" + numRows +
             ", fingerprint='" + schemaFingerprint + '\'' +
             '}';
    }
  }
}
