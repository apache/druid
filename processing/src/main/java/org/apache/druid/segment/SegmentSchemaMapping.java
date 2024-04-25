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
import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact representation of segment schema for multiple segments. Note, that this is a mutable class.
 */
public class SegmentSchemaMapping
{
  private final Map<String, SegmentMetadata> segmentIdToMetadataMap;

  private final Map<String, SchemaPayload> schemaFingerprintToPayloadMap;

  private final int schemaVersion;

  @JsonCreator
  public SegmentSchemaMapping(
      @JsonProperty("segmentIdToMetadataMap") Map<String, SegmentMetadata> segmentIdToMetadataMap,
      @JsonProperty("schemaFingerprintToPayloadMap") Map<String, SchemaPayload> schemaFingerprintToPayloadMap,
      @JsonProperty("schemaVersion") int schemaVersion
  )
  {
    this.segmentIdToMetadataMap = segmentIdToMetadataMap;
    this.schemaFingerprintToPayloadMap = schemaFingerprintToPayloadMap;
    this.schemaVersion = schemaVersion;
  }

  public SegmentSchemaMapping(int schemaVersion)
  {
    this.segmentIdToMetadataMap = new HashMap<>();
    this.schemaFingerprintToPayloadMap = new HashMap<>();
    this.schemaVersion = schemaVersion;
  }

  @JsonProperty
  public Map<String, SegmentMetadata> getSegmentIdToMetadataMap()
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
      SegmentId segmentId,
      SchemaPayloadPlus schemaPayloadPlus,
      String fingerprint
  )
  {
    segmentIdToMetadataMap.put(segmentId.toString(), new SegmentMetadata(schemaPayloadPlus.getNumRows(), fingerprint));
    schemaFingerprintToPayloadMap.put(fingerprint, schemaPayloadPlus.getSchemaPayload());
  }

  /**
   * Merge with another instance.
   */
  public void merge(SegmentSchemaMapping other)
  {
    this.segmentIdToMetadataMap.putAll(other.getSegmentIdToMetadataMap());
    this.schemaFingerprintToPayloadMap.putAll(other.getSchemaFingerprintToPayloadMap());
  }

  public int getSchemaCount()
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
    SegmentSchemaMapping that = (SegmentSchemaMapping) o;
    return schemaVersion == that.schemaVersion && Objects.equals(
        segmentIdToMetadataMap,
        that.segmentIdToMetadataMap
    ) && Objects.equals(schemaFingerprintToPayloadMap, that.schemaFingerprintToPayloadMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentIdToMetadataMap, schemaFingerprintToPayloadMap, schemaVersion);
  }

  @Override
  public String toString()
  {
    return "SegmentSchemaMapping{" +
           "segmentIdToMetadataMap=" + segmentIdToMetadataMap +
           ", schemaFingerprintToPayloadMap=" + schemaFingerprintToPayloadMap +
           ", version='" + schemaVersion + '\'' +
           '}';
  }
}
