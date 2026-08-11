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
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.DatasourceBaseTableMetadata;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.ModelProperties.GranularityPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.indexing.DataSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

  public static final String PROJECTIONS_KEYS_PROPERTY = "projections";

  /**
   * Physical layout of the base table. The layout combines with the declared column list (which remains the source of
   * truth for column names, types, and order) to derive the physical spec used to generate segments;
   *
   * @see DatasourceBaseTableMetadata#createSpec
   */
  public static final String BASE_TABLE_PROPERTY = "baseTable";

  /**
   * The set of existing columns to "delete" (actually, just hide) from the
   * SQL layer. Used to "remove" unwanted columns to avoid the need to rewrite
   * existing segments to accomplish the task.
   */
  public static final String HIDDEN_COLUMNS_PROPERTY = "hiddenColumns";

  public static final String SEALED_PROPERTY = "sealed";

  public static final String TABLE_TYPE = "datasource";

  public DatasourceDefn()
  {
    super(
        "Datasource",
        TABLE_TYPE,
        Arrays.asList(
            new SegmentGranularityFieldDefn(),
            new ModelProperties.IntPropertyDefn(TARGET_SEGMENT_ROWS_PROPERTY),
            new ClusterKeysDefn(),
            new HiddenColumnsDefn(),
            new ModelProperties.BooleanPropertyDefn(SEALED_PROPERTY),
            new ProjectionsDefn(),
            new BaseTableDefn()
        ),
        null
    );
  }

  @Override
  public void validate(ResolvedTable table)
  {
    super.validate(table);
    final DatasourceBaseTableMetadata baseTable = table.decodeProperty(BASE_TABLE_PROPERTY);
    if (baseTable != null) {
      // A base table layout derives the physical segment schema from the declared columns, so a column the query
      // produces but the table does not declare cannot be stored; require 'sealed' so ingestion rejects such columns
      // instead of silently dropping them. Requiring the flag allows us to someday support non-sealed definitions,
      // which could work by appending undeclared columns to the derived schema.
      if (!table.booleanProperty(SEALED_PROPERTY)) {
        throw InvalidInput.exception(
            "Datasource with a [%s] layout must also set [%s] to true; the declared columns define the physical"
            + " segment schema, so columns not declared in the table cannot be ingested",
            BASE_TABLE_PROPERTY,
            SEALED_PROPERTY
        );
      }
      // Cross-validate the layout against the declared columns by deriving the physical spec, so that catalog writes
      // fail fast instead of surfacing layout problems at ingest time.
      baseTable.createSpec(table.spec().columns());
    }
    validateProjections(table);
  }

  /**
   * Cross-validate the declared projections. Names must be unique, a projection must not be coarser than the segments
   * it lives in, and the types it groups by must agree with the types the table declares. For a sealed table the
   * declared columns are the whole schema, so a projection that reads a column the table does not declare can never be
   * built and is rejected; for a non-sealed table ingestion may add columns the catalog has not seen, so only the
   * projections' internal consistency is checked.
   */
  private void validateProjections(ResolvedTable table)
  {
    final List<DatasourceProjectionMetadata> projections = table.decodeProperty(PROJECTIONS_KEYS_PROPERTY);
    if (projections == null || projections.isEmpty()) {
      return;
    }

    final List<AggregateProjectionSpec> specs = new ArrayList<>(projections.size());
    for (DatasourceProjectionMetadata projection : projections) {
      if (projection == null || projection.getSpec() == null) {
        throw InvalidInput.exception("Projections must each have a [spec]");
      }
      specs.add(projection.getSpec());
    }

    final String granularity = table.stringProperty(SEGMENT_GRANULARITY_PROPERTY);
    DataSchema.validateProjections(
        specs,
        granularity == null ? null : CatalogUtils.asDruidGranularity(granularity)
    );

    validateProjectionGroupingTypes(table, specs);

    if (!table.booleanProperty(SEALED_PROPERTY) || table.spec().columns() == null) {
      return;
    }
    final Set<String> declared = new HashSet<>(CatalogUtils.columnNames(table.spec().columns()));
    declared.add(Columns.TIME_COLUMN);
    for (AggregateProjectionSpec spec : specs) {
      final Set<String> available = new HashSet<>(declared);
      for (VirtualColumn virtualColumn : spec.getVirtualColumns().getVirtualColumns()) {
        available.add(virtualColumn.getOutputName());
      }
      for (String required : requiredColumns(spec)) {
        if (!available.contains(required)) {
          throw InvalidInput.exception(
              "Projection [%s] references column [%s], which table [%s] does not declare",
              spec.getName(),
              required,
              table.spec().type()
          );
        }
      }
    }
  }

  /**
   * Reconcile the types a projection groups by against the types the table declares. Grouping columns are the only
   * part of a projection that shares column components with the base table, such as dictionaries, so they are the only
   * part whose types have to agree with it. A grouping column that disagrees was built against a definition the table
   * no longer has.
   */
  private static void validateProjectionGroupingTypes(ResolvedTable table, List<AggregateProjectionSpec> specs)
  {
    final List<ColumnSpec> columns = table.spec().columns();
    if (columns == null) {
      return;
    }
    final Map<String, ColumnType> declaredTypes = new HashMap<>();
    for (ColumnSpec column : columns) {
      final ColumnType type = Columns.druidType(column);
      if (type != null) {
        declaredTypes.put(column.name(), type);
      }
    }
    for (AggregateProjectionSpec spec : specs) {
      for (DimensionSchema grouping : spec.getGroupingColumns()) {
        final ColumnType declared = declaredTypes.get(grouping.getName());
        if (declared != null && !declared.equals(grouping.getColumnType())) {
          throw InvalidInput.exception(
              "Projection [%s] groups on column [%s] as type [%s], but the table declares it as type [%s]. A"
              + " projection is built from the table's columns, so changing the type of a column requires redefining"
              + " the projections that group on it",
              spec.getName(),
              grouping.getName(),
              grouping.getColumnType(),
              declared
          );
        }
      }
    }
  }

  private static Set<String> requiredColumns(AggregateProjectionSpec spec)
  {
    final Set<String> required = new HashSet<>();
    for (VirtualColumn virtualColumn : spec.getVirtualColumns().getVirtualColumns()) {
      required.addAll(virtualColumn.requiredColumns());
    }
    for (DimensionSchema groupingColumn : spec.getGroupingColumns()) {
      required.add(groupingColumn.getName());
    }
    for (AggregatorFactory aggregator : spec.getAggregators()) {
      required.addAll(aggregator.requiredFields());
    }
    if (spec.getFilter() != null) {
      required.addAll(spec.getFilter().getRequiredColumns());
    }
    return required;
  }

  /**
   * Check if {@link TableSpec#type()} is {@link DatasourceDefn#TABLE_TYPE}
   */
  public static boolean isDatasource(String tableType)
  {
    return DatasourceDefn.TABLE_TYPE.equals(tableType);
  }

  public static boolean isDatasource(ResolvedTable table)
  {
    return table.defn() instanceof DatasourceDefn;
  }

  public static class SegmentGranularityFieldDefn extends GranularityPropertyDefn
  {
    public SegmentGranularityFieldDefn()
    {
      super(SEGMENT_GRANULARITY_PROPERTY);
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

  public static class ClusterKeysDefn extends ModelProperties.ListPropertyDefn<ClusterKeySpec>
  {
    public ClusterKeysDefn()
    {
      super(
          CLUSTER_KEYS_PROPERTY,
          "ClusterKeySpec list",
          new TypeReference<>() {}
      );
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return;
      }
      List<ClusterKeySpec> clusterKeys = decode(value, jsonMapper);
      for (ClusterKeySpec clusterKey : clusterKeys) {
        if (clusterKey.desc()) {
          throw new IAE(
              StringUtils.format("Cannot specify DESC clustering key [%s]. Only ASC is supported.", clusterKey)
          );
        }
      }
    }
  }

  public static class ProjectionsDefn extends ModelProperties.TypeRefPropertyDefn<List<DatasourceProjectionMetadata>>
  {
    public static final TypeReference<List<DatasourceProjectionMetadata>> TYPE_REF = new TypeReference<>() {};

    public ProjectionsDefn()
    {
      super(PROJECTIONS_KEYS_PROPERTY, "DatasourceProjectionMetadata list", TYPE_REF);
    }
  }

  public static class BaseTableDefn extends ModelProperties.TypeRefPropertyDefn<DatasourceBaseTableMetadata>
  {
    public static final TypeReference<DatasourceBaseTableMetadata> TYPE_REF = new TypeReference<>() {};

    public BaseTableDefn()
    {
      super(BASE_TABLE_PROPERTY, "DatasourceBaseTableMetadata", TYPE_REF);
    }
  }
}
