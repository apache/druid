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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.catalog.DatasourceColumnSpec.DetailColumnSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.DimensionSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.MeasureSpec;
import org.apache.druid.catalog.TableMetadata.TableType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Datasource metadata exchanged via the REST API and stored
 * in the catalog.
 */
public class DatasourceSpec extends TableSpec
{
  /**
   * Segment grain at ingestion and initial compaction. Aging rules
   * may override the value as segments age. If not provided here,
   * then it must be provided at ingestion time.
   */
  private final String segmentGranularity;

  /**
   * Ingestion and auto-compaction rollup granularity. If null, then no
   * rollup is enabled. Same as {@code queryGranularity} in and ingest spec,
   * but renamed since this granularity affects rollup, not queries. Can be
   * overridden at ingestion time. The grain may change as segments evolve:
   * this is the grain only for ingest.
   */
  private final String rollupGranularity;

  /**
   * The target segment size at ingestion and initial compaction.
   * If 0, then the system setting is used.
   */
  private final int targetSegmentRows;

  /**
   * Whether to enable auto-compaction. Only relevant if no auto-compaction
   * spec is defined, since the existence of a spec overrides this setting.
   */
  private final boolean enableAutoCompaction;

  /**
   * The offset of segments to be auto-compacted relative to the current
   * time. If not present, the auto-compaction default is used if
   * auto-compaction is enabled.
   */
  private final String autoCompactionDelay;

  private final List<DatasourceColumnSpec> columns;

  public DatasourceSpec(
      @JsonProperty("segmentGranularity") String segmentGranularity,
      @JsonProperty("rollupGranularity") String rollupGranularity,
      @JsonProperty("targetSegmentRows") int targetSegmentRows,
      @JsonProperty("enableAutoCompaction") boolean enableAutoCompaction,
      @JsonProperty("autoCompactionDelay") String autoCompactionDelay,
      @JsonProperty("properties") Map<String, Object> properties,
      @JsonProperty("columns") List<DatasourceColumnSpec> columns
  )
  {
    super(properties);

    // Note: no validation here. If a bad definition got into the
    // DB, don't prevent deserialization.

    this.segmentGranularity = segmentGranularity;
    this.rollupGranularity = rollupGranularity;
    this.targetSegmentRows = targetSegmentRows;
    this.enableAutoCompaction = enableAutoCompaction;
    this.autoCompactionDelay = autoCompactionDelay;
    this.columns = columns == null ? Collections.emptyList() : columns;
  }

  @Override
  public TableType type()
  {
    return TableType.DATASOURCE;
  }

  @JsonProperty("rollupGranularity")
  @JsonInclude(Include.NON_NULL)
  public String rollupGranularity()
  {
    return rollupGranularity;
  }

  @JsonProperty("segmentGranularity")
  @JsonInclude(Include.NON_NULL)
  public String segmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty("targetSegmentRows")
  @JsonInclude(Include.NON_DEFAULT)
  public int targetSegmentRows()
  {
    return targetSegmentRows;
  }

  @JsonProperty("enableAutoCompaction")
  @JsonInclude(Include.NON_DEFAULT)
  public boolean enableAutoCompaction()
  {
    return enableAutoCompaction;
  }

  @JsonProperty("autoCompactionDelay")
  @JsonInclude(Include.NON_NULL)
  public String autoCompactionDelay()
  {
    return autoCompactionDelay;
  }

  @JsonProperty("columns")
  @JsonInclude(Include.NON_EMPTY)
  public List<DatasourceColumnSpec> columns()
  {
    return columns;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
  }

  @JsonIgnore
  public boolean isDetail()
  {
    return Strings.isNullOrEmpty(rollupGranularity);
  }

  @JsonIgnore
  public boolean isRollup()
  {
    return !isDetail();
  }

  @Override
  public void validate()
  {
    super.validate();
    if (Strings.isNullOrEmpty(segmentGranularity)) {
      throw new IAE("Segment granularity is required.");
    }
    boolean isDetail = isDetail();
    Set<String> names = new HashSet<>();
    for (ColumnSpec col : columns) {
      if (isDetail && col instanceof MeasureSpec) {
        throw new IAE(StringUtils.format(
            "Measure column %s not allowed for a detail table",
            col.name()));
      }
      if (isDetail && col instanceof DimensionSpec) {
        throw new IAE(StringUtils.format(
            "Dimension column %s not allowed for a detail table",
            col.name()));
      }
      if (!isDetail && col instanceof DetailColumnSpec) {
        throw new IAE(StringUtils.format(
            "Detail column %s not allowed for a rollup table",
            col.name()));
      }
      col.validate();
      if (!names.add(col.name())) {
        throw new IAE("Duplicate column name: " + col.name());
      }
    }
  }

  @Override
  public String defaultSchema()
  {
    return TableId.DRUID_SCHEMA;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    DatasourceSpec other = (DatasourceSpec) o;
    return Objects.equals(this.segmentGranularity, other.segmentGranularity)
        && Objects.equals(this.rollupGranularity, other.rollupGranularity)
        && this.targetSegmentRows == other.targetSegmentRows
        && this.enableAutoCompaction == other.enableAutoCompaction
        && Objects.equals(this.autoCompactionDelay, other.autoCompactionDelay)
        && Objects.equals(this.columns, other.columns)
        && Objects.equals(this.properties(), other.properties());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        segmentGranularity,
        rollupGranularity,
        targetSegmentRows,
        enableAutoCompaction,
        autoCompactionDelay,
        columns,
        properties());
  }

  public static class Builder
  {
    private String segmentGranularity;
    private String rollupGranularity;
    private int targetSegmentRows;
    private boolean enableAutoCompaction;
    private String autoCompactionDelay;
    private List<DatasourceColumnSpec> columns;
    private Map<String, Object> properties;

    public Builder()
    {
      this.columns = new ArrayList<>();
      this.properties = new HashMap<>();
    }

    public Builder(DatasourceSpec defn)
    {
      this.segmentGranularity = defn.segmentGranularity;
      this.rollupGranularity = defn.rollupGranularity;
      this.targetSegmentRows = defn.targetSegmentRows;
      this.enableAutoCompaction = defn.enableAutoCompaction;
      this.autoCompactionDelay = defn.autoCompactionDelay;
      this.properties = new HashMap<>(defn.properties());
      this.columns = new ArrayList<>(defn.columns);
    }

    public Builder rollupGranularity(String rollupGranularty)
    {
      this.rollupGranularity = rollupGranularty;
      return this;
    }

    public Builder segmentGranularity(String segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Builder targetSegmentRows(int targetSegmentRows)
    {
      this.targetSegmentRows = targetSegmentRows;
      return this;
    }

    public Builder enableAutoCompaction(boolean enableAutoCompaction)
    {
      this.enableAutoCompaction = enableAutoCompaction;
      return this;
    }

    public Builder autoCompactionDelay(String autoCompactionDelay)
    {
      this.autoCompactionDelay = autoCompactionDelay;
      return this;
    }

    public List<DatasourceColumnSpec> columns()
    {
      return columns;
    }

    public Builder column(DatasourceColumnSpec column)
    {
      if (Strings.isNullOrEmpty(column.name())) {
        throw new IAE("Column name is required");
      }
      columns.add(column);
      return this;
    }

    public Builder timeColumn()
    {
      return column("__time", "TIMESTAMP");
    }

    public Builder column(String name, String sqlType)
    {
      if (rollupGranularity == null) {
        column(new DetailColumnSpec(name, sqlType));
      } else {
        column(new DimensionSpec(name, sqlType));
      }
      return this;
    }

    public Builder measure(String name, String sqlType, String aggFn)
    {
      return column(new MeasureSpec(name, sqlType, aggFn));
    }

    public Builder properties(Map<String, Object> properties)
    {
      this.properties = properties;
      return this;
    }

    public Builder property(String key, Object value)
    {
      if (properties == null) {
        properties = new HashMap<>();
      }
      properties.put(key, value);
      return this;
    }

    public Map<String, Object> properties()
    {
      return properties;
    }

    public DatasourceSpec build()
    {
      if (targetSegmentRows < 0) {
        targetSegmentRows = 0;
      }
      // TODO(paul): validate upper bound
      return new DatasourceSpec(
          segmentGranularity,
          rollupGranularity,
          targetSegmentRows,
          enableAutoCompaction,
          autoCompactionDelay,
          properties,
          columns);
    }
  }
}
