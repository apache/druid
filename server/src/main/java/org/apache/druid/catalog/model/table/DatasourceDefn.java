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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.ModelProperties.GranularityPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Arrays;
import java.util.List;

public class DatasourceDefn extends TableDefn
{
  /**
   * Segment grain at ingestion and initial compaction. Aging rules
   * may override the value as segments age. If not provided here,
   * then it must be provided at ingestion time.
   */
  public static final String SEGMENT_GRANULARITY_PROPERTY = "segmentGranularity";

  /**
   * Catalog property value for the "all time" granularity.
   */
  public static final String ALL_GRANULARITY = "ALL";

  /**
   * The target segment size at ingestion and initial compaction.
   * If unset, then the system setting is used.
   */
  public static final String TARGET_SEGMENT_ROWS_PROPERTY = "targetSegmentRows";

  /**
   * The clustering column names and sort order for each new segment.
   */
  public static final String CLUSTER_KEYS_PROPERTY = "clusterKeys";

  /**
   * The set of existing columns to "delete" (actually, just hide) from the
   * SQL layer. Used to "remove" unwanted columns to avoid the need to rewrite
   * existing segments to accomplish the task.
   */
  public static final String HIDDEN_COLUMNS_PROPERTY = "hiddenColumns";

  public static final String SEALED_PROPERTY = "sealed";

  public static final String TABLE_TYPE = "datasource";

  public static class SegmentGranularityFieldDefn extends GranularityPropertyDefn
  {
    public SegmentGranularityFieldDefn()
    {
      super(SEGMENT_GRANULARITY_PROPERTY);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      String gran = decode(value, jsonMapper);
      if (Strings.isNullOrEmpty(gran)) {
        throw new IAE("Segment granularity is required.");
      }
      CatalogUtils.validateGranularity(gran);
    }
  }

  public static class HiddenColumnsDefn extends StringListPropertyDefn
  {
    public HiddenColumnsDefn()
    {
      super(HIDDEN_COLUMNS_PROPERTY);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return;
      }
      List<String> hiddenColumns = decode(value, jsonMapper);
      for (String col : hiddenColumns) {
        if (Columns.TIME_COLUMN.equals(col)) {
          throw new IAE(
              StringUtils.format("Cannot hide column %s", col)
          );
        }
      }
    }
  }

  public DatasourceDefn()
  {
    super(
        "Datasource",
        TABLE_TYPE,
        Arrays.asList(
            new SegmentGranularityFieldDefn(),
            new ModelProperties.IntPropertyDefn(TARGET_SEGMENT_ROWS_PROPERTY),
            new ModelProperties.ListPropertyDefn<ClusterKeySpec>(
                CLUSTER_KEYS_PROPERTY,
                "cluster keys",
                new TypeReference<List<ClusterKeySpec>>() { }
            ),
            new HiddenColumnsDefn(),
            new ModelProperties.BooleanPropertyDefn(SEALED_PROPERTY)
        ),
        null
    );
  }

  @Override
  protected void validateColumn(ColumnSpec spec)
  {
    super.validateColumn(spec);
    if (Columns.isTimeColumn(spec.name()) && spec.dataType() != null) {
      // Validate type in next PR
    }
  }

  public static boolean isDatasource(String tableType)
  {
    return DatasourceDefn.TABLE_TYPE.equals(tableType);
  }

  public static boolean isDatasource(ResolvedTable table)
  {
    return table.defn() instanceof DatasourceDefn;
  }
}
