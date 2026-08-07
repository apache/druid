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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class ClusteredValueGroupsBaseTableSchemaTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final String TENANT_COL = "tenant";
  private static final String REGION_COL = "region";
  private static final String PRIORITY_COL = "priority";
  private static final String METRIC_COL = "metric";
  private static final RowSignature TENANT_SIGNATURE = RowSignature.builder()
                                                                   .add(TENANT_COL, ColumnType.STRING)
                                                                   .build();

  @Test
  void testSerdeMultiColumnClustering() throws JsonProcessingException
  {
    ClusteredValueGroupsBaseTableSchema schema = newSchema(
        RowSignature.builder()
                    .add(TENANT_COL, ColumnType.STRING)
                    .add(PRIORITY_COL, ColumnType.LONG)
                    .build()
    );

    Assertions.assertEquals(
        schema,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(schema), ProjectionSchema.class)
    );
  }

  @Test
  void testColumnNamesIncludeClusteringColumns()
  {
    Assertions.assertEquals(
        List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL, PRIORITY_COL, REGION_COL, METRIC_COL),
        newSchema(TENANT_SIGNATURE).getColumnNames()
    );
  }

  @Test
  void testDimensionNamesIncludeClusteringColumnsExceptTime()
  {
    // Summary's dimension names exclude __time but keep clustering columns (they're logical dims).
    Assertions.assertEquals(
        List.of(TENANT_COL, PRIORITY_COL, REGION_COL, METRIC_COL),
        newSchema(TENANT_SIGNATURE).getDimensionNames()
    );
  }

  @Test
  void testNullColumnsRejected()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            null,
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            TENANT_SIGNATURE,
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "clustered base table schema columns must not be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testNullOrderingRejected()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
            null,
            TENANT_SIGNATURE,
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "clustered base table schema ordering must not be null",
        t.getMessage()
    );
  }

  @Test
  void testEmptyClusteringColumnsRejected()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            RowSignature.empty(),
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "clustered base table schema clusteringColumns must not be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testClusteringColumnMustAppearInColumns()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            RowSignature.builder().add("unknown", ColumnType.STRING).build(),
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "clusteringColumn [unknown] must appear in columns of the clustered base table summary",
        t.getMessage()
    );
  }

  @Test
  void testUnsupportedClusteringTypeRejected()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, "tenants"),
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            RowSignature.builder().add("tenants", ColumnType.STRING_ARRAY).build(),
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "clustering column [tenants] has unsupported type [ARRAY<STRING>]; "
        + "allowed types are STRING, LONG, DOUBLE, FLOAT",
        t.getMessage()
    );
  }

  @Test
  void testSharedColumnsRoundTrip() throws JsonProcessingException
  {
    ClusteredValueGroupsBaseTableSchema schema = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL, REGION_COL),
        List.of(OrderBy.ascending(TENANT_COL), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        TENANT_SIGNATURE,
        List.of(REGION_COL),
        ClusteringDictionaries.EMPTY,
        null
    );

    ClusteredValueGroupsBaseTableSchema roundTripped = (ClusteredValueGroupsBaseTableSchema) JSON_MAPPER.readValue(
        JSON_MAPPER.writeValueAsString(schema),
        ProjectionSchema.class
    );
    Assertions.assertEquals(schema, roundTripped);
    Assertions.assertEquals(List.of(REGION_COL), roundTripped.getSharedColumns());
  }

  @Test
  void testSharedColumnMustAppearInColumns()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
            List.of(OrderBy.ascending(TENANT_COL), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            TENANT_SIGNATURE,
            List.of("unknown"),
            null,
            null
        )
    );
    Assertions.assertEquals(
        "sharedColumn [unknown] must appear in columns of the clustered base table summary",
        t.getMessage()
    );
  }

  @Test
  void testClusteringPrefixOfOrderingRequired()
  {
    // The first ordering position is __time, but the clustering column is tenant, must reject.
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending(TENANT_COL)),
            TENANT_SIGNATURE,
            null,
            null,
            null
        )
    );
    Assertions.assertTrue(t.getMessage().contains("clustering columns must form a prefix of the segment ordering"));
  }

  @Test
  void testClusteringRequiresOrderingAtLeastAsLongAsClusteringSize()
  {
    // Two clustering columns but ordering only has one entry, must reject.
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new ClusteredValueGroupsBaseTableSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL, REGION_COL),
            List.of(OrderBy.ascending(TENANT_COL)),
            RowSignature.builder()
                        .add(TENANT_COL, ColumnType.STRING)
                        .add(REGION_COL, ColumnType.STRING)
                        .build(),
            null,
            null,
            null
        )
    );
    Assertions.assertTrue(
        t.getMessage().contains("ordering size [1] must be at least clusteringColumns size [2]")
    );
  }

  @Test
  void testNullDictionariesDefaultsToEmpty()
  {
    // A null clusteringDictionaries is interpreted as "no values yet" (empty per-type dicts); valid for a
    // freshly-constructed clustered schema that doesn't have any groups defined.
    final ClusteredValueGroupsBaseTableSchema schema = new ClusteredValueGroupsBaseTableSchema(
        null,
        List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL),
        List.of(OrderBy.ascending(TENANT_COL), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        TENANT_SIGNATURE,
        null,
        null,
        null
    );
    Assertions.assertEquals(ClusteringDictionaries.EMPTY, schema.getClusteringDictionaries());
  }

  @Test
  void testJsonCreatorAcceptsUnsortedDictionary()
  {
    // The trusting JsonCreator path does NOT validate; segments produced by a well-behaved writer have already
    // passed validation, so re-checking on every load would just burn CPU.
    final ClusteringDictionaries dicts = new ClusteringDictionaries(List.of("b", "a"), null, null, null);
    Assertions.assertEquals(List.of("b", "a"), dicts.getStringDictionary());
  }

  @Test
  void testGetGroupOrderingDropsClusteringPrefix()
  {
    // Single clustering column → group ordering is the rest of the segment ordering.
    Assertions.assertEquals(
        List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending(REGION_COL)),
        newSchema(TENANT_SIGNATURE).getGroupOrdering()
    );

    // Two clustering columns → both prefix entries are dropped.
    final RowSignature multi = RowSignature.builder()
                                            .add(TENANT_COL, ColumnType.STRING)
                                            .add(PRIORITY_COL, ColumnType.LONG)
                                            .build();
    Assertions.assertEquals(
        List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending(REGION_COL)),
        newSchema(multi).getGroupOrdering()
    );
  }

  @Test
  void testGetGroupColumnNamesExcludesClusteringColumns()
  {
    // Summary's columns are (__time, tenant, priority, region, metric); clustering=tenant → group view drops tenant.
    Assertions.assertEquals(
        List.of(ColumnHolder.TIME_COLUMN_NAME, PRIORITY_COL, REGION_COL, METRIC_COL),
        newSchema(TENANT_SIGNATURE).getGroupColumnNames()
    );
  }

  @Test
  void testGetGroupDimensionNamesExcludesClusteringColumnsAndTime()
  {
    // Summary's dimension names are (__time excluded) (tenant, priority, region, metric);
    // clustering=tenant → drops tenant.
    Assertions.assertEquals(
        List.of(PRIORITY_COL, REGION_COL, METRIC_COL),
        newSchema(TENANT_SIGNATURE).getGroupDimensionNames()
    );
  }

  @Test
  void testToClusterGroupTuples()
  {
    // (tenant STRING, priority LONG) clustering with two groups: (acme, 5) and (globex, 10). toClusterGroupTuples
    // materializes one tuple per group from the dictionaries, in group order, for the broker/coordinator wire form.
    final RowSignature clustering = RowSignature.builder()
                                                .add(TENANT_COL, ColumnType.STRING)
                                                .add(PRIORITY_COL, ColumnType.LONG)
                                                .build();
    final ClusteringDictionaries dicts = new ClusteringDictionaries(
        List.of("acme", "globex"),
        List.of(5L, 10L),
        null,
        null
    );
    final ClusteredValueGroupsBaseTableSchema schema = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(TENANT_COL, PRIORITY_COL, ColumnHolder.TIME_COLUMN_NAME, REGION_COL),
        List.of(
            OrderBy.ascending(TENANT_COL),
            OrderBy.ascending(PRIORITY_COL),
            OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)
        ),
        clustering,
        null,
        dicts,
        List.of(
            new TableClusterGroupSpec(List.of(0, 0), 3),
            new TableClusterGroupSpec(List.of(1, 1), 7)
        )
    );

    final ClusterGroupTuples tuples = schema.toClusterGroupTuples();
    Assertions.assertEquals(clustering, tuples.clusteringColumns());
    Assertions.assertTrue(tuples.virtualColumns().isEmpty());
    Assertions.assertEquals(
        List.of(
            Arrays.asList("acme", 5L),
            Arrays.asList("globex", 10L)
        ),
        tuples.tuples()
    );
  }

  @Test
  void testToClusterGroupTuplesPreservesClusteringVirtualColumns()
  {
    // When the segment is clustered on a virtual column, the produced tuples carry those VCs so portable rules
    // can resolve them by equivalence.
    final VirtualColumns vcs = VirtualColumns.create(
        new ExpressionVirtualColumn("tenant_lower", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final RowSignature clustering = RowSignature.builder().add("tenant_lower", ColumnType.STRING).build();
    final ClusteredValueGroupsBaseTableSchema schema = new ClusteredValueGroupsBaseTableSchema(
        vcs,
        List.of("tenant_lower", ColumnHolder.TIME_COLUMN_NAME, REGION_COL),
        List.of(OrderBy.ascending("tenant_lower"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        clustering,
        null,
        new ClusteringDictionaries(List.of("acme"), null, null, null),
        List.of(new TableClusterGroupSpec(List.of(0), 1))
    );

    final ClusterGroupTuples tuples = schema.toClusterGroupTuples();
    Assertions.assertEquals(vcs, tuples.virtualColumns());
    Assertions.assertEquals(List.of(Collections.singletonList("acme")), tuples.tuples());
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(ClusteredValueGroupsBaseTableSchema.class)
                  .withIgnoredFields("timeColumnPosition", "effectiveGranularity")
                  .usingGetClass()
                  .verify();
  }

  private static ClusteredValueGroupsBaseTableSchema newSchema(RowSignature clusteringColumns)
  {
    // clustering columns must be a prefix of the segment ordering. Build the ordering by prefixing every clustering
    // column in order, then __time and the remaining data columns.
    final ArrayList<OrderBy> ordering = new ArrayList<>(clusteringColumns.size() + 2);
    for (String col : clusteringColumns.getColumnNames()) {
      ordering.add(OrderBy.ascending(col));
    }
    ordering.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
    ordering.add(OrderBy.ascending(REGION_COL));
    // No cluster groups in this fixture; dictionaries are empty (null is also acceptable, defaults to EMPTY).
    return new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(ColumnHolder.TIME_COLUMN_NAME, TENANT_COL, PRIORITY_COL, REGION_COL, METRIC_COL),
        ordering,
        clusteringColumns,
        null,
        ClusteringDictionaries.EMPTY,
        null
    );
  }
}
