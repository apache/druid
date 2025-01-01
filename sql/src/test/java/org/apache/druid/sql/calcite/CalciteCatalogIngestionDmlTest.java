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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.CalciteCatalogIngestionDmlTest.CatalogIngestionDmlComponentSupplier;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SqlTestFrameworkConfig.ComponentSupplier(CatalogIngestionDmlComponentSupplier.class)
public abstract class CalciteCatalogIngestionDmlTest extends CalciteIngestionDmlTest
{

  private static final Map<String, Object> CONTEXT_WITH_VALIDATION_DISABLED;

  static {
    CONTEXT_WITH_VALIDATION_DISABLED = new HashMap<>(DEFAULT_CONTEXT);
    CONTEXT_WITH_VALIDATION_DISABLED.put(QueryContexts.CATALOG_VALIDATION_ENABLED, false);
  }

  private final String operationName;
  private final String dmlPrefixPattern;

  public CalciteCatalogIngestionDmlTest()
  {
    this.operationName = getOperationName();
    this.dmlPrefixPattern = getDmlPrefixPattern();
  }

  public abstract String getOperationName();
  public abstract String getDmlPrefixPattern();

  public static class CatalogIngestionDmlComponentSupplier extends IngestionDmlComponentSupplier
  {
    public CatalogIngestionDmlComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    private static final ObjectMapper MAPPER = new DefaultObjectMapper();
    public static ImmutableMap<String, DatasourceTable> RESOLVED_TABLES = ImmutableMap.of(
        "hourDs", new DatasourceTable(
            RowSignature.builder().addTimeColumn().build(),
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("hourDs"),
                RowSignature.builder().addTimeColumn().build(),
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "hourDs",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H"),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(
                    RowSignature.builder()
                        .addTimeColumn()
                        .build()),
                false
            )
        ),
        "noPartitonedBy", new DatasourceTable(
            RowSignature.builder().addTimeColumn().build(),
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("noPartitonedBy"),
                RowSignature.builder().addTimeColumn().build(),
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "noPartitonedBy",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(
                    RowSignature.builder()
                        .addTimeColumn()
                        .build()),
                false
            )
        ),
        "strictTableWithNoDefinedSchema", new DatasourceTable(
            RowSignature.builder().build(),
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("strictTableWithNoDefinedSchema"),
                RowSignature.builder().build(),
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "strictTableWithNoDefinedSchema",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(DatasourceDefn.SEALED_PROPERTY, true),
                        null
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(RowSignature.builder().build()),
                false
            )
        ),
        "foo", new DatasourceTable(
            FOO_TABLE_SIGNATURE,
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("foo"),
                FOO_TABLE_SIGNATURE,
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "foo",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                            new ColumnSpec("dim1", Columns.STRING, null),
                            new ColumnSpec("dim2", Columns.STRING, null),
                            new ColumnSpec("dim3", Columns.STRING, null),
                            new ColumnSpec("cnt", Columns.LONG, null),
                            new ColumnSpec("m1", Columns.FLOAT, null),
                            new ColumnSpec("m2", Columns.DOUBLE, null),
                            new ColumnSpec("unique_dim1", HyperUniquesAggregatorFactory.TYPE.asTypeString(), null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(FOO_TABLE_SIGNATURE),
                false
            )
        ),
        "fooSealed", new DatasourceTable(
            FOO_TABLE_SIGNATURE,
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("fooSealed"),
                FOO_TABLE_SIGNATURE,
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "fooSealed",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(DatasourceDefn.SEALED_PROPERTY, true),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                            new ColumnSpec("dim1", Columns.STRING, null),
                            new ColumnSpec("dim2", Columns.STRING, null),
                            new ColumnSpec("dim3", Columns.STRING, null),
                            new ColumnSpec("cnt", Columns.LONG, null),
                            new ColumnSpec("m1", Columns.FLOAT, null),
                            new ColumnSpec("m2", Columns.DOUBLE, null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(FOO_TABLE_SIGNATURE),
                false
            )
        ),
        "tableWithClustering", new DatasourceTable(
            FOO_TABLE_SIGNATURE,
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("tableWithClustering"),
                RowSignature.builder()
                    .addTimeColumn()
                    .add("dim1", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build(),
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "tableWithClustering",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(
                            DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "ALL",
                            DatasourceDefn.CLUSTER_KEYS_PROPERTY,
                            ImmutableList.of(
                                new ClusterKeySpec("dim1", false),
                                new ClusterKeySpec("dim2", false)
                            )
                        ),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                            new ColumnSpec("dim1", Columns.STRING, null),
                            new ColumnSpec("dim2", Columns.STRING, null),
                            new ColumnSpec("cnt", Columns.LONG, null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(RowSignature.builder()
                    .addTimeColumn()
                    .add("dim1", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build()),
                false
            )
        ),
        "tableWithClusteringDesc", new DatasourceTable(
            FOO_TABLE_SIGNATURE,
            new DatasourceTable.PhysicalDatasourceMetadata(
                new TableDataSource("tableWithClusteringDesc"),
                RowSignature.builder()
                    .addTimeColumn()
                    .add("dim1", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build(),
                false,
                false
            ),
            new DatasourceTable.EffectiveMetadata(
                new DatasourceFacade(new ResolvedTable(
                    new TableDefn(
                        "tableWithClusteringDesc",
                        DatasourceDefn.TABLE_TYPE,
                        null,
                        null
                    ),
                    new TableSpec(
                        DatasourceDefn.TABLE_TYPE,
                        ImmutableMap.of(
                            DatasourceDefn.CLUSTER_KEYS_PROPERTY,
                            ImmutableList.of(
                                new ClusterKeySpec("dim1", false),
                                new ClusterKeySpec("dim2", true)
                            )
                        ),
                        ImmutableList.of(
                            new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                            new ColumnSpec("dim1", Columns.STRING, null),
                            new ColumnSpec("dim2", Columns.STRING, null),
                            new ColumnSpec("cnt", Columns.LONG, null)
                        )
                    ),
                    MAPPER
                )),
                DatasourceTable.EffectiveMetadata.toEffectiveColumns(RowSignature.builder()
                    .addTimeColumn()
                    .add("dim1", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build()),
                false
            )
        )
    );

    @Override
    public CatalogResolver createCatalogResolver()
    {
      return new CatalogResolver.NullCatalogResolver() {
        @Nullable
        @Override
        public DruidTable resolveDatasource(
            final String tableName,
            final DatasourceTable.PhysicalDatasourceMetadata dsMetadata
        )
        {
          if (RESOLVED_TABLES.get(tableName) != null) {
            return RESOLVED_TABLES.get(tableName);
          }
          return dsMetadata == null ? null : new DatasourceTable(dsMetadata);
        }
      };
    }
  }

  /**
   * If the segment grain is given in the catalog and absent in the PARTITIONED BY clause in the query, then use the
   * value from the catalog.
   */
  @Test
  public void testInsertHourGrainPartitonedByFromCatalog()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "hourDs") + "\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "dim2", "dim3", "cnt", "m1", "m2", "unique_dim1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.ofComplex("hyperUnique"))
                .context(queryContextWithGranularity(Granularities.HOUR))
                .build()
        )
        .verify();
  }

  /**
   * If the segment grain is given in the catalog, and also by PARTITIONED BY, then
   * the query value is used.
   */
  @Test
  public void testInsertHourGrainWithDayPartitonedByFromQuery()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "hourDs") + "\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY day")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("hourDs", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceWrite("hourDs"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "dim2", "dim3", "cnt", "m1", "m2", "unique_dim1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.ofComplex("hyperUnique"))
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  /**
   * If the segment grain is absent in the catalog and absent in the PARTITIONED BY clause in the query, then
   * validation error.
   */
  @Test
  public void testInsertNoPartitonedByFromCatalog()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "noPartitonedBy") + "\n" +
             "SELECT * FROM foo")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidException.class,
            StringUtils.format("Operation [%s] requires a PARTITIONED BY to be explicitly defined, but none was found.", operationName)
        )
        .verify();
  }

  /**
   * If the segment grain is absent in the catalog, but given by PARTITIONED BY, then
   * the query value is used.
   */
  @Test
  public void testInsertNoPartitonedByWithDayPartitonedByFromQuery()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "noPartitonedBy") + "\n" +
             "SELECT * FROM foo\n" +
             "PARTITIONED BY day")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("noPartitonedBy", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceWrite("noPartitonedBy"), dataSourceRead("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "dim2", "dim3", "cnt", "m1", "m2", "unique_dim1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.ofComplex("hyperUnique"))
                .context(queryContextWithGranularity(Granularities.DAY))
                .build()
        )
        .verify();
  }

  /**
   * Adding a new column during ingestion that is not defined in a non-sealed table should succeed.
   */
  @Test
  public void testInsertAddNonDefinedColumnIntoNonSealedCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m2", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG),
                    expressionVirtualColumn("v2", "CAST(\"c\", 'DOUBLE')", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "CAST(\"d\", 'LONG')", ColumnType.LONG)
                )
                .columns("v0", "b", "v1", "v2", "v3", "e")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.LONG, ColumnType.DOUBLE, ColumnType.LONG, ColumnType.STRING)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition. Should use
   * the catalog defined clustering
   */
  @Test
  public void testInsertTableWithClusteringWithClusteringFromCatalog()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("dim2", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClustering") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("tableWithClustering", signature)
        .expectResources(dataSourceWrite("tableWithClustering"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG)
                )
                .orderBy(
                    ImmutableList.of(
                        OrderBy.ascending("b"),
                        OrderBy.ascending("d")
                    )
                )
                .columns("v0", "b", "d", "v1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but user specifies
   * clustering on the ingest query. Should use the query defined clustering
   */
  @Test
  public void testInsertTableWithClusteringWithClusteringFromQuery()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("dim2", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClustering") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "CLUSTERED BY dim1")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("tableWithClustering", signature)
        .expectResources(dataSourceWrite("tableWithClustering"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG)
                )
                .orderBy(
                    ImmutableList.of(
                        OrderBy.ascending("b")
                    )
                )
                .columns("v0", "b", "d", "v1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but user specifies
   * clustering on the ingest query on column that has not been defined in the table catalog definition.
   * Should use the query defined clustering
   */
  @Test
  public void testInsertTableWithClusteringWithClusteringOnNewColumnFromQuery()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("dim2", ColumnType.STRING)
        .add("dim3", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClustering") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  e AS dim3,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME\n" +
             "CLUSTERED BY dim3")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("tableWithClustering", signature)
        .expectResources(dataSourceWrite("tableWithClustering"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG)
                )
                .orderBy(
                    ImmutableList.of(
                        OrderBy.ascending("e")
                    )
                )
                .columns("v0", "b", "d", "e", "v1")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but user specifies
   * clustering on the ingest query on column that has not been specified in the select clause. Should fail with
   * validation error.
   */
  @Test
  public void testInsertTableWithQueryDefinedClusteringOnNonSelectedColumn()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClustering") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  e AS dim3,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME\n" +
             "CLUSTERED BY blah")
        .expectValidationError(
            DruidException.class,
            "Column 'blah' not found in any table (line [13], column [14])")
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but one of the clustering
   * columns specified has not been specified in the select clause. Should fail with validation error.
   */
  @Test
  public void testInsertTableWithCatalogDefinedClusteringOnNonSelectedColumn()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClustering") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  e AS dim3,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Column 'dim2' not found in any table (line [0], column [0])")
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but one of the clustering
   * columns specified has not been specified in the select clause. Should fail with validation error.
   */
  @Test
  public void testInsertTableWithCatalogDefinedClusteringDesc()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClusteringDesc") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  e AS dim3,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Invalid CLUSTERED BY clause [`dim2` DESC]: cannot sort in descending order.")
        .verify();
  }

  /**
   * Insert into a catalog table that has clustering defined on the table definition, but one of the clustering
   * columns specified has not been specified in the select clause. Should fail with validation error.
   */
  @Test
  public void testInsertTableWithQueryDefinedClusteringDesc()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "tableWithClusteringDesc") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  d AS dim2,\n" +
             "  e AS dim3,\n" +
             "  1 AS cnt\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME\n" +
             "CLUSTERED BY dim1 DESC")
        .expectValidationError(
            DruidException.class,
            "Invalid CLUSTERED BY clause [`dim1` DESC]: cannot sort in descending order.")
        .verify();
  }

  /**
   * Adding a new column during group by ingestion that is not defined in a non-sealed table should succeed.
   */
  @Test
  public void testGroupByInsertAddNonDefinedColumnIntoNonSealedCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m2", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .add("extra4_complex", ColumnType.LONG)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3,\n" +
             "  APPROX_COUNT_DISTINCT_BUILTIN(c) as extra4_complex\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "GROUP BY 1,2,3,4,5,6\n" +
             "PARTITIONED BY ALL TIME"
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            GroupByQuery.builder()
                .setDataSource(externalDataSource)
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG)
                )
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                        new DefaultDimensionSpec("b", "d1", ColumnType.STRING),
                        new DefaultDimensionSpec("c", "d3", ColumnType.LONG),
                        new DefaultDimensionSpec("d", "d4", ColumnType.LONG),
                        new DefaultDimensionSpec("e", "d5", ColumnType.STRING)
                    )
                )
                .setAggregatorSpecs(
                    new CardinalityAggregatorFactory(
                        "a0",
                        null,
                        ImmutableList.of(
                            new DefaultDimensionSpec(
                                "c",
                                "c",
                                ColumnType.LONG
                            )
                        ),
                        false,
                        true
                    )
                )
                .setPostAggregatorSpecs(
                    expressionPostAgg("p0", "1", ColumnType.LONG),
                    expressionPostAgg("p1", "CAST(\"d3\", 'DOUBLE')", ColumnType.DOUBLE)
                )
                .setContext(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Adding a new column during ingestion that is not defined in a sealed table should fail with
   * proper validation error.
   */
  @Test
  public void testInsertAddNonDefinedColumnIntoSealedCatalogTable()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "fooSealed") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  d AS extra2\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectValidationError(
            DruidException.class,
            "Column [extra2] is not defined in the target table [druid.fooSealed] strict schema"
        )
        .verify();
  }

  /**
   * Adding a new column during ingestion that is not defined in a sealed table, when catalog validation is disabled,
   * should plan accordingly.
   */
  @Test
  public void testInsertAddNonDefinedColumnIntoSealedCatalogTableAndValidationDisabled()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m2", ColumnType.LONG)
        .add("extra2", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .context(CONTEXT_WITH_VALIDATION_DISABLED)
        .sql(StringUtils.format(dmlPrefixPattern, "fooSealed") + "\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  d AS extra2\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("fooSealed", signature)
        .expectResources(dataSourceWrite("fooSealed"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG)
                )
                .columns("v0", "b", "v1", "c", "d")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.LONG, ColumnType.LONG, ColumnType.STRING)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }


  /**
   * Inserting into a catalog table with a WITH source succeeds
   */
  @Test
  public void testInsertWithSourceIntoCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m2", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n" +
             "WITH \"ext\" AS (\n" +
             "  SELECT *\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             ")\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3\n" +
             "FROM \"ext\"\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(externalDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn("v1", "1", ColumnType.LONG),
                    expressionVirtualColumn("v2", "CAST(\"c\", 'DOUBLE')", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "CAST(\"d\", 'LONG')", ColumnType.LONG)
                )
                .columns("v0", "b", "v1", "v2", "v3", "e")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.LONG, ColumnType.DOUBLE, ColumnType.LONG, ColumnType.STRING)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Adding a new column during group by ingestion that is not defined in a non-sealed table should succeed.
   */
  @Test
  public void testGroupByInsertWithSourceIntoCatalogTable()
  {
    ExternalDataSource externalDataSource = new ExternalDataSource(
        new InlineInputSource("2022-12-26T12:34:56,extra,10,\"20\",foo\n"),
        new CsvInputFormat(ImmutableList.of("a", "b", "c", "d", "e"), null, false, false, 0, null),
        RowSignature.builder()
            .add("a", ColumnType.STRING)
            .add("b", ColumnType.STRING)
            .add("c", ColumnType.LONG)
            .add("d", ColumnType.STRING)
            .add("e", ColumnType.STRING)
            .build()
    );
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING)
        .add("cnt", ColumnType.LONG)
        .add("m2", ColumnType.DOUBLE)
        .add("extra2", ColumnType.LONG)
        .add("extra3", ColumnType.STRING)
        .add("extra4_complex", ColumnType.LONG)
        .build();
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n" +
             "WITH \"ext\" AS (\n" +
             "  SELECT *\n" +
             "FROM TABLE(inline(\n" +
             "  data => ARRAY['2022-12-26T12:34:56,extra,10,\"20\",foo'],\n" +
             "  format => 'csv'))\n" +
             "  (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e VARCHAR)\n" +
             ")\n" +
             "SELECT\n" +
             "  TIME_PARSE(a) AS __time,\n" +
             "  b AS dim1,\n" +
             "  1 AS cnt,\n" +
             "  c AS m2,\n" +
             "  CAST(d AS BIGINT) AS extra2,\n" +
             "  e AS extra3,\n" +
             "  APPROX_COUNT_DISTINCT_BUILTIN(c) as extra4_complex\n" +
             "FROM \"ext\"\n" +
             "GROUP BY 1,2,3,4,5,6\n" +
             "PARTITIONED BY ALL TIME"
        )
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceWrite("foo"), Externals.externalRead("EXTERNAL"))
        .expectQuery(
            GroupByQuery.builder()
                .setDataSource(externalDataSource)
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_parse(\"a\",null,'UTC')", ColumnType.LONG)
                )
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                        new DefaultDimensionSpec("b", "d1", ColumnType.STRING),
                        new DefaultDimensionSpec("c", "d3", ColumnType.LONG),
                        new DefaultDimensionSpec("d", "d4", ColumnType.LONG),
                        new DefaultDimensionSpec("e", "d5", ColumnType.STRING)
                    )
                )
                .setAggregatorSpecs(
                    new CardinalityAggregatorFactory(
                        "a0",
                        null,
                        ImmutableList.of(
                            new DefaultDimensionSpec(
                                "c",
                                "c",
                                ColumnType.LONG
                            )
                        ),
                        false,
                        true
                    )
                )
                .setPostAggregatorSpecs(
                    expressionPostAgg("p0", "1", ColumnType.LONG),
                    expressionPostAgg("p1", "CAST(\"d3\", 'DOUBLE')", ColumnType.DOUBLE)
                )
                .setContext(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testInsertIntoExistingStrictNoDefinedSchema()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "strictTableWithNoDefinedSchema") + " SELECT __time AS __time FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Column [__time] is not defined in the target table [druid.strictTableWithNoDefinedSchema] strict schema")
        .verify();
  }

  /**
   * Assigning a column during ingestion, to an input type that is not compatible with the defined type of the
   * column, should result in a proper validation error.
   */
  @Test
  public void testInsertIntoExistingWithIncompatibleTypeAssignment()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n"
             + "SELECT\n"
             + "  __time AS __time,\n"
             + "  ARRAY[dim1] AS dim1\n"
             + "FROM foo\n"
             + "PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Cannot assign to target field 'dim1' of type VARCHAR from source field 'dim1' of type VARCHAR ARRAY (line [4], column [3])")
        .verify();
  }

  /**
   * Assigning a column during ingestion, to an input type that is not compatible with the defined type of the
   * column, when catalog validation is disabled, should plan accordingly.
   */
  @Test
  public void testInsertIntoExistingWithIncompatibleTypeAssignmentAndValidationDisabled()
  {
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim1", ColumnType.STRING_ARRAY)
        .build();
    testIngestionQuery()
        .context(CONTEXT_WITH_VALIDATION_DISABLED)
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n"
             + "SELECT\n"
             + "  __time AS __time,\n"
             + "  ARRAY[dim1] AS dim1\n"
             + "FROM foo\n"
             + "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "array(\"dim1\")", ColumnType.STRING_ARRAY)
                )
                .columns("__time", "v0")
                .columnTypes(ColumnType.LONG, ColumnType.STRING_ARRAY)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  /**
   * Assigning a complex type column during ingestion, to an input type that is not compatible with the defined type of
   * the column, should result in a proper validation error.
   */
  @Test
  public void testGroupByInsertIntoExistingWithIncompatibleTypeAssignment()
  {
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n"
             + "SELECT\n"
             + "  __time AS __time,\n"
             + "  ARRAY[dim1] AS unique_dim1\n"
             + "FROM foo\n"
             + "PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "Cannot assign to target field 'unique_dim1' of type COMPLEX<hyperUnique> from source field 'unique_dim1' of type VARCHAR ARRAY (line [4], column [3])")
        .verify();
  }

  /**
   * Assigning a complex type column during ingestion, to an input type that is not compatible with the defined type of
   * the column, when catalog validation is disabled, should plan accordingly.
   */
  @Test
  public void testGroupByInsertIntoExistingWithIncompatibleTypeAssignmentAndValidationDisabled()
  {
    final RowSignature signature = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("unique_dim1", ColumnType.STRING_ARRAY)
        .build();
    testIngestionQuery()
        .context(CONTEXT_WITH_VALIDATION_DISABLED)
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n"
             + "SELECT\n"
             + "  __time AS __time,\n"
             + "  ARRAY[dim1] AS unique_dim1\n"
             + "FROM foo\n"
             + "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("foo", signature)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "array(\"dim1\")", ColumnType.STRING_ARRAY)
                )
                .columns("__time", "v0")
                .columnTypes(ColumnType.LONG, ColumnType.STRING_ARRAY)
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void testWindowingErrorWithEngineFeatureOff()
  {
    assumeFalse(queryFramework().engine().featureAvailable(EngineFeature.WINDOW_FUNCTIONS));
    testIngestionQuery()
        .sql(StringUtils.format(dmlPrefixPattern, "foo") + "\n"
             + "SELECT dim1, ROW_NUMBER() OVER () from foo\n"
             + "PARTITIONED BY ALL TIME")
        .expectValidationError(
            DruidException.class,
            "The query contains window functions; They are not supported on engine[ingestion-test]. (line [2], column [14])"
        )
        .verify();
  }
}
