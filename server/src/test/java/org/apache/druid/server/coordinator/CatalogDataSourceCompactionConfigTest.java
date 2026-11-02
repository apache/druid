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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.MapMetadataCatalog;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.model.ClusteredValueGroupsBaseTableMetadata;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.BaseTableProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class CatalogDataSourceCompactionConfigTest
{
  private static final String TEST_DS = "test";
  private static final String TEST_DS_PROJECTIONS_ONLY_SCHEMA = "test_projections";
  private static final String TEST_DS_CLUSTERED = "test_clustered";
  private static final String TEST_DS_CLUSTERED_UNSEALED = "test_clustered_unsealed";
  private static final String TEST_DS_NO_SEGMENT_GRANULARITY = "test_no_segment_granularity";
  private static final String TEST_DS_CLUSTER_KEYS = "test_cluster_keys";
  private static final String TEST_DS_NUMERIC_CLUSTER_KEY = "test_numeric_cluster_key";
  private static final String TEST_DS_UNDECLARED_CLUSTER_KEY = "test_undeclared_cluster_key";
  private static final String TEST_DS_ARRAY_CLUSTER_KEY = "test_array_cluster_key";
  private static final String TEST_DS_UNTYPED_CLUSTER_KEY = "test_untyped_cluster_key";
  private static final String TEST_DS_SEGMENT_ONLY_CLUSTER_KEY = "test_segment_only_cluster_key";
  private static final String TEST_DS_SEALED_SEGMENT_ONLY_CLUSTER_KEY = "test_sealed_segment_only_cluster_key";
  private static final String TEST_DS_CLUSTERED_WITH_KEYS = "test_clustered_with_keys";

  private static final List<ColumnSpec> CLUSTERED_COLUMNS = ImmutableList.of(
      new ColumnSpec("tenant", ColumnType.STRING.asTypeString(), null),
      new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
      new ColumnSpec("string", ColumnType.STRING.asTypeString(), null)
  );

  private static final ObjectMapper MAPPER;
  private static final MapMetadataCatalog METADATA_CATALOG;

  private static final AggregateProjectionSpec TEST_PROJECTION_SPEC_1 =
      AggregateProjectionSpec.builder("string_sum_long_hourly")
                             .virtualColumns(
                                 Granularities.toVirtualColumn(
                                     Granularities.HOUR,
                                     Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                 )
                             )
                             .groupingColumns(new StringDimensionSchema("string"))
                             .aggregators(new LongSumAggregatorFactory("sum_long", "long"))
                             .build();

  static {
    MAPPER = new DefaultObjectMapper();
    METADATA_CATALOG = new MapMetadataCatalog(MAPPER);
    MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(MetadataCatalog.class, METADATA_CATALOG)
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), MAPPER)
    );

    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS),
        TableBuilder.datasource(TEST_DS, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
                            new ColumnSpec("string", ColumnType.STRING.asTypeString(), null),
                            new ColumnSpec("double", ColumnType.DOUBLE.asTypeString(), null),
                            new ColumnSpec("long", ColumnType.LONG.asTypeString(), null)
                        )
                    )
                    .property(
                        DatasourceDefn.PROJECTIONS_KEYS_PROPERTY,
                        ImmutableList.of(
                            new DatasourceProjectionMetadata(TEST_PROJECTION_SPEC_1)
                        )
                    )
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_PROJECTIONS_ONLY_SCHEMA),
        TableBuilder.datasource(TEST_DS_PROJECTIONS_ONLY_SCHEMA, "P1D")
                    .property(
                        DatasourceDefn.PROJECTIONS_KEYS_PROPERTY,
                        ImmutableList.of(
                            new DatasourceProjectionMetadata(TEST_PROJECTION_SPEC_1)
                        )
                    )
                    .buildSpec()

    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_CLUSTERED),
        TableBuilder.datasource(TEST_DS_CLUSTERED, "P1D")
                    .columns(CLUSTERED_COLUMNS)
                    .baseTable(new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null))
                    .sealed(true)
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_CLUSTERED_UNSEALED),
        TableBuilder.datasource(TEST_DS_CLUSTERED_UNSEALED, "P1D")
                    .columns(CLUSTERED_COLUMNS)
                    .baseTable(new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null))
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_CLUSTER_KEYS),
        TableBuilder.datasource(TEST_DS_CLUSTER_KEYS, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
                            new ColumnSpec("string", ColumnType.STRING.asTypeString(), null),
                            new ColumnSpec("string2", ColumnType.STRING.asTypeString(), null)
                        )
                    )
                    .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 1000)
                    .clusterColumns(new ClusterKeySpec("string", false), new ClusterKeySpec("string2", false))
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_NUMERIC_CLUSTER_KEY),
        TableBuilder.datasource(TEST_DS_NUMERIC_CLUSTER_KEY, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
                            new ColumnSpec("string", ColumnType.STRING.asTypeString(), null),
                            new ColumnSpec("double", ColumnType.DOUBLE.asTypeString(), null)
                        )
                    )
                    .clusterColumns(new ClusterKeySpec("string", false), new ClusterKeySpec("double", false))
                    .buildSpec()
    );
    // A base table layout plus a cluster key on a column the table does not declare. Unsealed, that column can still
    // be discovered from the segments at compaction time; sealed, the declaration is final and it cannot.
    for (String ds : List.of(TEST_DS_SEGMENT_ONLY_CLUSTER_KEY, TEST_DS_SEALED_SEGMENT_ONLY_CLUSTER_KEY)) {
      final TableBuilder builder =
          TableBuilder.datasource(ds, "P1D")
                      .columns(CLUSTERED_COLUMNS)
                      .baseTable(new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null))
                      .clusterColumns(new ClusterKeySpec("segment_only", false));
      if (TEST_DS_SEALED_SEGMENT_ONLY_CLUSTER_KEY.equals(ds)) {
        builder.sealed(true);
      }
      METADATA_CATALOG.addSpec(TableId.datasource(ds), builder.buildSpec());
    }
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_ARRAY_CLUSTER_KEY),
        TableBuilder.datasource(TEST_DS_ARRAY_CLUSTER_KEY, "P1D")
                    // declared with the SQL spellings a catalog table actually uses
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, Columns.SQL_TIMESTAMP, null),
                            new ColumnSpec("tags", Columns.SQL_VARCHAR_ARRAY, null)
                        )
                    )
                    .clusterColumns(new ClusterKeySpec("tags", false))
                    .buildSpec()
    );
    // A cluster key on a declared column that carries no type. Catalog tables created through the API can hold these,
    // and the catalog reads an absent type as a string.
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_UNTYPED_CLUSTER_KEY),
        TableBuilder.datasource(TEST_DS_UNTYPED_CLUSTER_KEY, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, Columns.SQL_TIMESTAMP, null),
                            new ColumnSpec("untyped", null, null)
                        )
                    )
                    .clusterColumns(new ClusterKeySpec("untyped", false))
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_UNDECLARED_CLUSTER_KEY),
        TableBuilder.datasource(TEST_DS_UNDECLARED_CLUSTER_KEY, "P1D")
                    .columns(
                        ImmutableList.of(
                            new ColumnSpec(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG.asTypeString(), null),
                            new ColumnSpec("string", ColumnType.STRING.asTypeString(), null)
                        )
                    )
                    .clusterColumns(new ClusterKeySpec("not_declared", false))
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_CLUSTERED_WITH_KEYS),
        TableBuilder.datasource(TEST_DS_CLUSTERED_WITH_KEYS, "P1D")
                    .columns(CLUSTERED_COLUMNS)
                    .baseTable(new ClusteredValueGroupsBaseTableMetadata(ImmutableList.of("tenant"), null, null))
                    .clusterColumns(new ClusterKeySpec("string", false))
                    .buildSpec()
    );
    METADATA_CATALOG.addSpec(
        TableId.datasource(TEST_DS_NO_SEGMENT_GRANULARITY),
        TableBuilder.datasource(TEST_DS_NO_SEGMENT_GRANULARITY, "P1D")
                    .columns(CLUSTERED_COLUMNS)
                    .property(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, null)
                    .buildSpec()
    );
  }

  private static CatalogDataSourceCompactionConfig configFor(String dataSource)
  {
    return new CatalogDataSourceCompactionConfig(
        dataSource,
        null,
        null,
        null,
        null,
        null,
        null,
        METADATA_CATALOG
    );
  }

  @Test
  public void testBaseTableIsDerivedFromCatalogLayoutAndColumns()
  {
    // The layout carries only what the column list cannot express (the clustering prefix); the declared columns,
    // in their declared order, are the schema.
    final BaseTableProjectionSpec baseTable = configFor(TEST_DS_CLUSTERED).getBaseTable();

    Assertions.assertInstanceOf(ClusteredValueGroupsBaseTableProjectionSpec.class, baseTable);
    Assertions.assertEquals(
        ImmutableList.of("tenant", ColumnHolder.TIME_COLUMN_NAME, "string"),
        baseTable.getDimensionsSpec()
                 .getDimensions()
                 .stream()
                 .map(DimensionSchema::getName)
                 .collect(Collectors.toList())
    );
    Assertions.assertEquals(
        ImmutableList.of("tenant"),
        ((ClusteredValueGroupsBaseTableProjectionSpec) baseTable).getClusteringColumnNames()
    );
  }

  @Test
  public void testBaseTableIsNullWhenTableDeclaresNoLayout()
  {
    Assertions.assertNull(configFor(TEST_DS).getBaseTable());
  }

  @Test
  public void testBaseTableIsNullWhenTableIsNotInCatalog()
  {
    Assertions.assertNull(configFor("not_in_catalog").getBaseTable());
  }

  @Test
  public void testIsSealedFollowsCatalogProperty()
  {
    Assertions.assertTrue(configFor(TEST_DS_CLUSTERED).isSealed());
    // An absent 'sealed' property means the table accepts columns it does not declare, so compaction must analyze the
    // existing segments to find and preserve them.
    Assertions.assertFalse(configFor(TEST_DS_CLUSTERED_UNSEALED).isSealed());
    Assertions.assertFalse(configFor(TEST_DS).isSealed());
    // No table at all: there is no baseTable either, so nothing observes this; report the safe default.
    Assertions.assertTrue(configFor("not_in_catalog").isSealed());
  }

  @Test
  public void testSegmentGranularityIsNullWhenTableDeclaresNone()
  {
    // The coordinator asks for this on every evaluation, so a table with no segmentGranularity must not blow up.
    Assertions.assertNull(configFor(TEST_DS_NO_SEGMENT_GRANULARITY).getSegmentGranularity());
    Assertions.assertNull(configFor(TEST_DS_NO_SEGMENT_GRANULARITY).getGranularitySpec().getSegmentGranularity());
    Assertions.assertNull(configFor("not_in_catalog").getSegmentGranularity());
  }

  @Test
  public void testTuningConfigPartitionsByClusterKeys()
  {
    // Cluster keys are the table's statement of how rows should be laid out, so compaction range-partitions by them
    // rather than falling back to dynamic partitioning.
    final PartitionsSpec partitionsSpec = configFor(TEST_DS_CLUSTER_KEYS).getTuningConfig().getPartitionsSpec();

    Assertions.assertInstanceOf(DimensionRangePartitionsSpec.class, partitionsSpec);
    Assertions.assertEquals(
        ImmutableList.of("string", "string2"),
        ((DimensionRangePartitionsSpec) partitionsSpec).getPartitionDimensions()
    );
    // targetSegmentRows is the segment size, carried as maxRowsPerSegment so it is used verbatim rather than scaled
    // by 1.5x the way a targetRowsPerSegment would be -- an INSERT into this table sizes segments the same way.
    Assertions.assertNull(((DimensionRangePartitionsSpec) partitionsSpec).getTargetRowsPerSegment());
    Assertions.assertEquals(1000, partitionsSpec.getMaxRowsPerSegment());
  }

  @Test
  public void testTuningConfigPartitionsByClusterKeysAlongsideBaseTable()
  {
    // A base table's clustering columns group rows within a segment; cluster keys split rows across segments. A table
    // that declares both gets both.
    final PartitionsSpec partitionsSpec =
        configFor(TEST_DS_CLUSTERED_WITH_KEYS).getTuningConfig().getPartitionsSpec();

    Assertions.assertEquals(
        ImmutableList.of("string"),
        ((DimensionRangePartitionsSpec) partitionsSpec).getPartitionDimensions()
    );
    Assertions.assertNotNull(configFor(TEST_DS_CLUSTERED_WITH_KEYS).getBaseTable());
  }

  @Test
  public void testTuningConfigFallsBackToDynamicPartitioning()
  {
    final PartitionsSpec partitionsSpec = configFor(TEST_DS).getTuningConfig().getPartitionsSpec();

    Assertions.assertInstanceOf(DynamicPartitionsSpec.class, partitionsSpec);
    Assertions.assertNull(((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows());
    Assertions.assertNull(configFor("not_in_catalog").getTuningConfig());
  }

  @Test
  public void testValidateRejectsNonStringClusterKeyForMsq()
  {
    // A catalog table with no baseTable carries no schema for the generic validation to read partition column types
    // from, so without this check a numeric cluster key would be accepted here and fail on every submitted task.
    final CompactionConfigValidationResult result = configFor(TEST_DS_NUMERIC_CLUSTER_KEY)
        .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null));

    Assertions.assertFalse(result.isValid());
    Assertions.assertTrue(result.getReason().contains("[double]"), result.getReason());
    Assertions.assertTrue(result.getReason().contains("string columns only"), result.getReason());
  }

  @Test
  public void testValidateRejectsArrayClusterKeyForMsq()
  {
    final CompactionConfigValidationResult result = configFor(TEST_DS_ARRAY_CLUSTER_KEY)
        .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null));

    Assertions.assertFalse(result.isValid());
    Assertions.assertTrue(result.getReason().contains("[tags]"), result.getReason());
  }

  @Test
  public void testValidateAcceptsStringClusterKeys()
  {
    Assertions.assertTrue(
        configFor(TEST_DS_CLUSTER_KEYS)
            .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null))
            .isValid()
    );
  }

  @Test
  public void testValidateIgnoresClusterKeyTypesForNativeEngine()
  {
    // Native range partitioning reads the string form of whatever column it is given, so the VARCHAR requirement is
    // the MSQ engine's, not a property of every compaction.
    Assertions.assertTrue(
        configFor(TEST_DS_NUMERIC_CLUSTER_KEY)
            .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.NATIVE, null))
            .isValid()
    );
  }

  @Test
  public void testValidateDefersUnknownClusterKeyTypeForUnsealedBaseTable()
  {
    // The generic MSQ validation sees only the base table's declared dimensions, so without deferral it would read
    // this key's missing type as non-string and reject a configuration that compaction can actually run: the unsealed
    // table infers the column from the segments, and the type is checked again at task time.
    Assertions.assertTrue(
        configFor(TEST_DS_SEGMENT_ONLY_CLUSTER_KEY)
            .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null))
            .isValid()
    );
  }

  @Test
  public void testValidateRejectsUnknownClusterKeyTypeForSealedBaseTable()
  {
    // Sealed means the declaration is the whole schema, so nothing will be inferred and the key can never resolve.
    final CompactionConfigValidationResult result = configFor(TEST_DS_SEALED_SEGMENT_ONLY_CLUSTER_KEY)
        .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null));

    Assertions.assertFalse(result.isValid());
    Assertions.assertTrue(result.getReason().contains("segment_only"), result.getReason());
  }

  @Test
  public void testValidateAllowsClusterKeyOnUndeclaredColumn()
  {
    // A non-sealed table may store columns it does not declare, so a cluster key naming one is legitimate and its
    // type is not knowable from the catalog.
    Assertions.assertTrue(
        configFor(TEST_DS_UNDECLARED_CLUSTER_KEY)
            .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null))
            .isValid()
    );
  }

  @Test
  public void testValidateAllowsClusterKeyOnUntypedColumn()
  {
    // The catalog reads a column declared without a type as a string, both when it builds a row signature and when it
    // builds a base table layout, so range partitioning by one is exactly what this check is looking for. Rejecting
    // it here would refuse a configuration that compaction runs correctly.
    Assertions.assertTrue(
        configFor(TEST_DS_UNTYPED_CLUSTER_KEY)
            .validate(new ClusterCompactionConfig(null, null, null, null, CompactionEngine.MSQ, null))
            .isValid()
    );
  }

  @Test
  public void testProjections()
  {
    final CatalogDataSourceCompactionConfig config = new CatalogDataSourceCompactionConfig(
        TEST_DS,
        null,
        null,
        null,
        null,
        null,
        null,
        METADATA_CATALOG
    );

    Assertions.assertEquals(
        TEST_PROJECTION_SPEC_1,
        config.getProjections().get(0)
    );
  }

  @Test
  public void testUndeclaredProjectionsAreEmptyOnlyWhenSealed()
  {
    // A sealed table is the complete description of the datasource, so declaring no projections declares that it has
    // none and compaction drops any the segments still carry. An unsealed table has said nothing about them, so they
    // stay null and compaction leaves them alone.
    Assertions.assertEquals(List.of(), configFor(TEST_DS_CLUSTERED).getProjections());
    Assertions.assertNull(configFor(TEST_DS_CLUSTERED_UNSEALED).getProjections());
    Assertions.assertNull(configFor("not_in_catalog").getProjections());
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final CatalogDataSourceCompactionConfig config = new CatalogDataSourceCompactionConfig(
        "foo",
        null,
        null,
        null,
        null,
        null,
        null,
        METADATA_CATALOG
    );

    Assertions.assertEquals(
        config,
        MAPPER.readValue(MAPPER.writeValueAsString(config), DataSourceCompactionConfig.class)
    );
  }

  @Test
  public void testSerdeWithSkipIntervals() throws JsonProcessingException
  {
    final Period skipOffsetFromLatest = new Period("PT1H");
    final List<Interval> skipIntervals = List.of(
        Intervals.of("2024-01-01/2024-01-02"),
        Intervals.of("2024-02-15/2024-02-16")
    );

    final CatalogDataSourceCompactionConfig config = new CatalogDataSourceCompactionConfig(
        "foo",
        null,
        skipOffsetFromLatest,
        skipIntervals,
        null,
        null,
        null,
        METADATA_CATALOG
    );

    final CatalogDataSourceCompactionConfig deserialized =
        (CatalogDataSourceCompactionConfig) MAPPER.readValue(
            MAPPER.writeValueAsString(config),
            DataSourceCompactionConfig.class
        );

    Assertions.assertEquals(config, deserialized);
    Assertions.assertEquals(skipOffsetFromLatest, deserialized.getSkipOffsetFromLatest());
    Assertions.assertEquals(skipIntervals, deserialized.getSkipIntervals());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(CatalogDataSourceCompactionConfig.class)
                  .usingGetClass()
                  .withIgnoredFields("catalog", "tableId")
                  .verify();
  }
}
