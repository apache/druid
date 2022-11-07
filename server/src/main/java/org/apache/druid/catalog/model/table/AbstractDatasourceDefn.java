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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnDefn;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.ModelProperties.GranularityPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class AbstractDatasourceDefn extends TableDefn
{
  /**
   * Segment grain at ingestion and initial compaction. Aging rules
   * may override the value as segments age. If not provided here,
   * then it must be provided at ingestion time.
   */
  public static final String SEGMENT_GRANULARITY_PROPERTY = "segmentGranularity";

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
      validateGranularity(gran);
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

  public AbstractDatasourceDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn<?>> properties,
      final List<ColumnDefn> columnDefns
  )
  {
    super(
        name,
        typeValue,
        CatalogUtils.concatLists(
            Arrays.asList(
                new SegmentGranularityFieldDefn(),
                new ModelProperties.IntPropertyDefn(TARGET_SEGMENT_ROWS_PROPERTY),
                new ModelProperties.ListPropertyDefn<ClusterKeySpec>(
                    CLUSTER_KEYS_PROPERTY,
                    "cluster keys",
                    new TypeReference<List<ClusterKeySpec>>() { }
                ),
                new HiddenColumnsDefn()
            ),
            properties
        ),
        columnDefns
    );
  }

  public static boolean isDatasource(String tableType)
  {
    return DatasourceDefn.TABLE_TYPE.equals(tableType);
  }

  public static boolean isDatasource(ResolvedTable table)
  {
    return table.defn() instanceof AbstractDatasourceDefn;
  }

  public static Set<String> tableTypes()
  {
    return ImmutableSet.of(
        DatasourceDefn.TABLE_TYPE
    );
  }
}
