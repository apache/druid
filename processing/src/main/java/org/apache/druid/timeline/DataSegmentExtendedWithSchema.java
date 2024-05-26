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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataSegmentExtendedWithSchema extends DataSegment
{
  @Nullable
  private final SchemaPayloadPlus schemaPayloadPlus;

  @Nullable
  private final String schemaFingerprint;

  @Nullable
  private final Integer schemaVersion;

  @JsonCreator
  public DataSegmentExtendedWithSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization to prevent dependency pollution
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JsonProperty("schemaPayloadPlus") @Nullable SchemaPayloadPlus schemaPayloadPlus,
      @JsonProperty("fingerprint") @Nullable String schemaFingerprint,
      @JsonProperty("schemaVersion") @Nullable Integer schemaVersion,
      @JacksonInject PruneSpecsHolder pruneSpecsHolder
  )
  {
    super(dataSource, interval, version, loadSpec, dimensions, metrics, shardSpec, lastCompactionState, binaryVersion, size, pruneSpecsHolder);
    this.schemaPayloadPlus = schemaPayloadPlus;
    this.schemaFingerprint = schemaFingerprint;
    this.schemaVersion = schemaVersion;
  }

  public DataSegmentExtendedWithSchema(
      DataSegment dataSegment,
      @Nullable SchemaPayloadPlus schemaPayloadPlus,
      @Nullable String schemaFingerprint,
      @Nullable Integer schemaVersion
  )
  {
    super(
        dataSegment.getDataSource(),
        dataSegment.getInterval(),
        dataSegment.getVersion(),
        dataSegment.getLoadSpec(),
        dataSegment.getDimensions(),
        dataSegment.getMetrics(),
        dataSegment.getShardSpec(),
        dataSegment.getBinaryVersion(),
        dataSegment.getSize()
    );
    this.schemaPayloadPlus = schemaPayloadPlus;
    this.schemaFingerprint = schemaFingerprint;
    this.schemaVersion = schemaVersion;
  }

  @JsonProperty("schemaPayloadPlus")
  @Nullable
  public SchemaPayloadPlus getSchemaPayloadPlus()
  {
    return schemaPayloadPlus;
  }

  @JsonProperty("fingerprint")
  @Nullable
  public String getSchemaFingerprint()
  {
    return schemaFingerprint;
  }

  @JsonProperty("schemaVersion")
  @Nullable
  public Integer getSchemaVersion()
  {
    return schemaVersion;
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
    if (!super.equals(o)) {
      return false;
    }
    DataSegmentExtendedWithSchema that = (DataSegmentExtendedWithSchema) o;
    return Objects.equals(schemaPayloadPlus, that.schemaPayloadPlus)
           && Objects.equals(schemaFingerprint, that.schemaFingerprint)
           && Objects.equals(schemaVersion, that.schemaVersion);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), schemaPayloadPlus, schemaFingerprint, schemaVersion);
  }
}
