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
import org.apache.druid.catalog.model.ColumnDefn;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.Properties;
import org.apache.druid.catalog.model.Properties.GranularityPropertyDefn;
import org.apache.druid.catalog.model.Properties.PropertyDefn;
import org.apache.druid.catalog.model.Properties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DatasourceDefn extends TableDefn
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
  public static final String CLUSTER_KEYS_PROPERTY = "clusterKeys";
  public static final String HIDDEN_COLUMNS_PROPERTY = "hiddenColumns";

  /**
   * Ingestion and auto-compaction rollup granularity. If null, then no
   * rollup is enabled. Same as {@code queryGranularity} in and ingest spec,
   * but renamed since this granularity affects rollup, not queries. Can be
   * overridden at ingestion time. The grain may change as segments evolve:
   * this is the grain only for ingest.
   */
  public static final String ROLLUP_GRANULARITY_PROPERTY = "rollupGranularity";

  public static final String DETAIL_DATASOURCE_TYPE = "detail";
  public static final String ROLLUP_DATASOURCE_TYPE = "rollup";

  public static final String DETAIL_COLUMN_TYPE = "detail";
  public static final String DIMENSION_TYPE = "dimension";
  public static final String MEASURE_TYPE = "measure";
  public static final String INPUT_COLUMN_TYPE = "input";

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

  /**
   * Definition of a column in a detail (non-rollup) datasource.
   */
  public static class DetailColumnDefn extends ColumnDefn
  {
    public DetailColumnDefn()
    {
      super(
          "Column",
          DETAIL_COLUMN_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  /**
   * Definition of a dimension in a rollup datasource.
   */
  public static class DimensionDefn extends ColumnDefn
  {
    public DimensionDefn()
    {
      super(
          "Dimension",
          DIMENSION_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  /**
   * Definition of a measure (metric) column.
   * Types are expressed as compound types: "AGG_FN(ARG_TYPE,...)"
   * where "AGG_FN" is one of the supported aggregate functions,
   * and "ARG_TYPE" is zero or more argument types.
   */
  public static class MeasureDefn extends ColumnDefn
  {
    public MeasureDefn()
    {
      super(
          "Measure",
          MEASURE_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      if (spec.sqlType() == null) {
        throw new IAE("A type is required for measure column " + spec.name());
      }
      if (Columns.isTimeColumn(spec.name())) {
        throw new IAE(StringUtils.format(
            "%s column cannot be a measure",
            Columns.TIME_COLUMN
            ));
      }
      MeasureTypes.parse(spec.sqlType());
    }
  }

  public static class DetailDatasourceDefn extends DatasourceDefn
  {
    public DetailDatasourceDefn()
    {
      super(
          "Detail datasource",
          DETAIL_DATASOURCE_TYPE,
          null,
          Collections.singletonList(new DetailColumnDefn())
      );
    }
  }

  public static class RollupDatasourceDefn extends DatasourceDefn
  {
    public RollupDatasourceDefn()
    {
      super(
          "Rollup datasource",
          ROLLUP_DATASOURCE_TYPE,
          Collections.singletonList(
              new Properties.GranularityPropertyDefn(ROLLUP_GRANULARITY_PROPERTY)
          ),
          Arrays.asList(
              new DimensionDefn(),
              new MeasureDefn()
          )
      );
    }
  }

  public DatasourceDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn> properties,
      final List<ColumnDefn> columnDefns
  )
  {
    super(
        name,
        typeValue,
        CatalogUtils.concatLists(
            Arrays.asList(
                new SegmentGranularityFieldDefn(),
                new Properties.IntPropertyDefn(TARGET_SEGMENT_ROWS_PROPERTY),
                new Properties.ListPropertyDefn<ClusterKeySpec>(
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
    return DETAIL_DATASOURCE_TYPE.equals(tableType)
        || ROLLUP_DATASOURCE_TYPE.equals(tableType);
  }

  public static boolean isDatasource(ResolvedTable table)
  {
    return table.defn() instanceof DatasourceDefn;
  }

  public static boolean isMeasure(ColumnSpec col)
  {
    return DatasourceDefn.MEASURE_TYPE.equals(col.type());
  }

  public static Set<String> tableTypes()
  {
    return CatalogUtils.setOf(
        DatasourceDefn.DETAIL_DATASOURCE_TYPE,
        DatasourceDefn.ROLLUP_DATASOURCE_TYPE
    );
  }
}
