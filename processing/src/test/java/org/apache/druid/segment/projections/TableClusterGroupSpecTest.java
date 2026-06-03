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
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TableClusterGroupSpecTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  // Column names that appear in more than one test fixture.
  private static final String COL_TENANT = "tenant";
  private static final String COL_REGION = "region";
  private static final String COL_PRIORITY = "priority";

  // Clustering-value literals used by more than one test.
  private static final String VAL_ACME = "acme";
  private static final String VAL_GLOBEX = "globex";
  private static final String VAL_US_EAST_1 = "us-east-1";
  private static final String VAL_US_WEST_2 = "us-west-2";

  // Common clustering signatures reused across tests.
  private static final RowSignature TENANT_CLUSTER_SIGNATURE =
      RowSignature.builder().add(COL_TENANT, ColumnType.STRING).build();
  private static final RowSignature TENANT_REGION_CLUSTER_SIGNATURE =
      RowSignature.builder()
                  .add(COL_TENANT, ColumnType.STRING)
                  .add(COL_REGION, ColumnType.STRING)
                  .build();

  /** Schema + the specs it wraps, returned together so tests can assert on both. */
  private record Built(ClusteredValueGroupsBaseTableSchema schema, List<TableClusterGroupSpec> specs)
  {
  }

  /** Build a schema with one cluster group per supplied typed tuple. */
  private static Built buildSummary(RowSignature clusteringColumns, List<? extends List<?>> tuples)
  {
    final ClusterGroupSchemaTestHelpers.Built built =
        ClusterGroupSchemaTestHelpers.buildClusterGroups(clusteringColumns, tuples);
    final List<String> dataColumns = List.of(ColumnHolder.TIME_COLUMN_NAME, COL_REGION, "metric");
    final ArrayList<String> allColumns = new ArrayList<>(clusteringColumns.getColumnNames());
    allColumns.addAll(dataColumns);
    final ArrayList<OrderBy> ordering = new ArrayList<>();
    for (String col : clusteringColumns.getColumnNames()) {
      ordering.add(OrderBy.ascending(col));
    }
    ordering.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
    ordering.add(OrderBy.ascending(COL_REGION));
    final ClusteredValueGroupsBaseTableSchema schema = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        allColumns,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        ordering,
        clusteringColumns,
        null,
        built.dictionaries(),
        built.specs()
    );
    return new Built(schema, built.specs());
  }

  /** Typed-tuple convenience: {@link Arrays#asList} allows null entries (unlike {@link List#of}). */
  private static List<?> tuple(Object... values)
  {
    return Arrays.asList(values);
  }

  /** Round-trip via the enclosing summary (specs aren't ProjectionSchema subtypes); returns specs[0]. */
  private static TableClusterGroupSpec roundTrip(ClusteredValueGroupsBaseTableSchema sum) throws JsonProcessingException
  {
    final String json = JSON_MAPPER.writeValueAsString(sum);
    final ClusteredValueGroupsBaseTableSchema deserialized =
        (ClusteredValueGroupsBaseTableSchema) JSON_MAPPER.readValue(json, ProjectionSchema.class);
    return deserialized.getClusterGroups().get(0);
  }

  @Test
  void testDictionariesAndIdsResolveFromTuples()
  {
    // Both columns are STRING, so their values share one merged dict.
    final Built b = buildSummary(
        TENANT_REGION_CLUSTER_SIGNATURE,
        List.of(
            tuple(VAL_GLOBEX, VAL_US_EAST_1),
            tuple(VAL_ACME, VAL_US_WEST_2),
            tuple(VAL_ACME, VAL_US_EAST_1)
        )
    );
    Assertions.assertEquals(
        List.of(VAL_ACME, VAL_GLOBEX, VAL_US_EAST_1, VAL_US_WEST_2),
        b.schema().getClusteringDictionaries().getStringDictionary()
    );
    Assertions.assertEquals(List.of(), b.schema().getClusteringDictionaries().getLongDictionary());
    Assertions.assertEquals(List.of(1, 2), b.specs().get(0).getClusteringValueIds());
    Assertions.assertEquals(List.of(0, 3), b.specs().get(1).getClusteringValueIds());
    Assertions.assertEquals(List.of(0, 2), b.specs().get(2).getClusteringValueIds());
    Assertions.assertArrayEquals(new Object[]{VAL_GLOBEX, VAL_US_EAST_1}, b.specs().get(0).lookupClusteringValues());
    Assertions.assertArrayEquals(new Object[]{VAL_ACME, VAL_US_WEST_2}, b.specs().get(1).lookupClusteringValues());
    Assertions.assertArrayEquals(new Object[]{VAL_ACME, VAL_US_EAST_1}, b.specs().get(2).lookupClusteringValues());
  }

  @Test
  void testDictionarySortsNullsFirst()
  {
    // Null is placed at position 0 in the STRING dictionary, with non-nulls following in ascending order.
    final Built b = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple((Object) null), tuple(VAL_ACME)));
    final List<String> stringDict = b.schema().getClusteringDictionaries().getStringDictionary();
    Assertions.assertEquals(2, stringDict.size());
    Assertions.assertNull(stringDict.get(0));
    Assertions.assertEquals(VAL_ACME, stringDict.get(1));
    Assertions.assertEquals(List.of(0), b.specs().get(0).getClusteringValueIds());
    Assertions.assertEquals(List.of(1), b.specs().get(1).getClusteringValueIds());
  }

  @Test
  void testSerdeStringValue() throws JsonProcessingException
  {
    final Built b = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple(VAL_ACME)));
    Assertions.assertEquals(b.specs().get(0), roundTrip(b.schema()));
  }

  @Test
  void testSerdeLongValueCoercedFromJsonNumber() throws JsonProcessingException
  {
    final Built b = buildSummary(
        RowSignature.builder().add(COL_PRIORITY, ColumnType.LONG).build(),
        List.of(tuple(5L))
    );
    final TableClusterGroupSpec roundTripped = roundTrip(b.schema());
    Assertions.assertEquals(b.specs().get(0), roundTripped);
    Assertions.assertEquals(Long.class, roundTripped.lookupClusteringValues()[0].getClass());
    Assertions.assertEquals(5L, roundTripped.lookupClusteringValues()[0]);
  }

  @Test
  void testSerdeFloatValue() throws JsonProcessingException
  {
    final Built b = buildSummary(
        RowSignature.builder().add("ratio", ColumnType.FLOAT).build(),
        List.of(tuple(0.5f))
    );
    final TableClusterGroupSpec roundTripped = roundTrip(b.schema());
    Assertions.assertEquals(b.specs().get(0), roundTripped);
    Assertions.assertEquals(Float.class, roundTripped.lookupClusteringValues()[0].getClass());
    Assertions.assertEquals(0.5f, roundTripped.lookupClusteringValues()[0]);
  }

  @Test
  void testSerdeMultiColumnWithSpecialCharsAndMixedTypes() throws JsonProcessingException
  {
    final Built b = buildSummary(
        RowSignature.builder()
                    .add(COL_TENANT, ColumnType.STRING)
                    .add(COL_PRIORITY, ColumnType.LONG)
                    .build(),
        List.of(tuple("A/B", 5L))
    );
    Assertions.assertEquals(b.specs().get(0), roundTrip(b.schema()));
  }

  @Test
  void testMixedTypeMultiGroupDictionaryRouting()
  {
    final Built b = buildSummary(
        RowSignature.builder()
                    .add("device_id", ColumnType.LONG)
                    .add("region", ColumnType.STRING)
                    .build(),
        List.of(
            tuple(202L, "us-east-1"),
            tuple(101L, "us-west-2"),
            tuple(101L, "us-east-1")
        )
    );
    Assertions.assertEquals(List.of(101L, 202L), b.schema().getClusteringDictionaries().getLongDictionary());
    Assertions.assertEquals(
        List.of("us-east-1", "us-west-2"),
        b.schema().getClusteringDictionaries().getStringDictionary()
    );
    // IDs are positions in each column's typed dict; LONG for position 0, STRING for position 1.
    Assertions.assertEquals(List.of(1, 0), b.specs().get(0).getClusteringValueIds());
    Assertions.assertEquals(List.of(0, 1), b.specs().get(1).getClusteringValueIds());
    Assertions.assertEquals(List.of(0, 0), b.specs().get(2).getClusteringValueIds());
    Assertions.assertArrayEquals(new Object[]{202L, "us-east-1"}, b.specs().get(0).lookupClusteringValues());
    Assertions.assertArrayEquals(new Object[]{101L, "us-west-2"}, b.specs().get(1).lookupClusteringValues());
    Assertions.assertArrayEquals(new Object[]{101L, "us-east-1"}, b.specs().get(2).lookupClusteringValues());
  }

  @Test
  void testJsonShapeIsOnlyClusteringValueIds() throws JsonProcessingException
  {
    // Spec carries only the clustering value ids (+ type tag); summary-owned fields must not leak in. The
    // per-group fields use compact wire names ("groups", "ids") since group entries repeat per group.
    final Built b = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple(VAL_ACME)));
    final String json = JSON_MAPPER.writeValueAsString(b.schema());
    Assertions.assertTrue(json.contains("\"groups\""), "summary serializes groups");
    Assertions.assertTrue(json.contains("\"clusteringDictionaries\""), "summary serializes clusteringDictionaries");
    Assertions.assertTrue(json.contains("\"ids\":[0]"), "spec carries clustering value ids");
    // Any of the summary-only fields appearing inside the spec object would mean we duplicated them.
    final int specStart = json.indexOf("\"groups\":[");
    final int specEnd = json.indexOf("]", specStart);
    final String specJson = json.substring(specStart, specEnd + 1);
    Assertions.assertFalse(specJson.contains("\"columns\""), "spec must not carry columns");
    Assertions.assertFalse(specJson.contains("\"ordering\""), "spec must not carry ordering");
    Assertions.assertFalse(specJson.contains("\"clusteringColumns\""), "spec must not carry clusteringColumns");
    Assertions.assertFalse(specJson.contains("\"aggregators\""), "spec must not carry aggregators");
    Assertions.assertFalse(specJson.contains("\"virtualColumns\""), "spec must not carry virtualColumns");
    // serialized form is dictionary IDs, not typed values: spec must not contain the literal value.
    Assertions.assertFalse(specJson.contains("\"" + VAL_ACME + "\""), "spec must not carry typed values inline");
  }

  @Test
  void testNullClusteringValueRoundTrips() throws JsonProcessingException
  {
    final Built b = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple((Object) null)));
    final TableClusterGroupSpec roundTripped = roundTrip(b.schema());
    Assertions.assertEquals(b.specs().get(0), roundTripped);
    Assertions.assertNull(roundTripped.lookupClusteringValues()[0]);
  }

  @Test
  void testMixedNullAndValueClusteringRoundTrips() throws JsonProcessingException
  {
    final Built b = buildSummary(TENANT_REGION_CLUSTER_SIGNATURE, List.of(tuple(VAL_ACME, null)));
    Assertions.assertEquals(b.specs().get(0), roundTrip(b.schema()));
  }

  @Test
  void testMismatchedTupleSizeRejectedByHelper()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> buildSummary(TENANT_REGION_CLUSTER_SIGNATURE, List.of(tuple(VAL_ACME)))
    );
    Assertions.assertTrue(t.getMessage().contains("must match clusteringColumns size"));
  }

  @Test
  void testGettersBeforeSetSummaryThrow()
  {
    // Spec without a summary back-reference: summary-dependent getters throw.
    final TableClusterGroupSpec spec = new TableClusterGroupSpec(List.of(0), null);
    Assertions.assertThrows(DruidException.class, spec::lookupClusteringValues);
    Assertions.assertThrows(DruidException.class, spec::getSummary);
  }

  @Test
  void testSetSummaryTwiceRejected()
  {
    final Built b = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple(VAL_ACME)));
    final Built other = buildSummary(TENANT_CLUSTER_SIGNATURE, List.of(tuple(VAL_GLOBEX)));
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> b.specs().get(0).setSummary(other.schema())
    );
    Assertions.assertTrue(t.getMessage().contains("summary already set"));
  }

  @Test
  void testEqualsAndHashcode()
  {
    final ClusteredValueGroupsBaseTableSchema redSummary = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(COL_TENANT, ColumnHolder.TIME_COLUMN_NAME),
        null,
        List.of(OrderBy.ascending(COL_TENANT), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        TENANT_CLUSTER_SIGNATURE,
        null,
        ClusteringDictionaries.EMPTY,
        null
    );
    final ClusteredValueGroupsBaseTableSchema blueSummary = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(COL_PRIORITY, ColumnHolder.TIME_COLUMN_NAME),
        null,
        List.of(OrderBy.ascending(COL_PRIORITY), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        RowSignature.builder().add(COL_PRIORITY, ColumnType.LONG).build(),
        null,
        ClusteringDictionaries.EMPTY,
        null
    );
    EqualsVerifier.forClass(TableClusterGroupSpec.class)
                  .withIgnoredFields("summary", "numRows")
                  .withPrefabValues(ClusteredValueGroupsBaseTableSchema.class, redSummary, blueSummary)
                  .suppress(Warning.NONFINAL_FIELDS)
                  .usingGetClass()
                  .verify();
  }
}
