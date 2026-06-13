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

package org.apache.druid.segment.incremental;

import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class OnHeapClusteredBaseTableTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  private static MapBasedInputRow row(long ts, Map<String, Object> values)
  {
    final Map<String, Object> event = new HashMap<>(values);
    event.put("ts", ts);
    return new MapBasedInputRow(ts, List.copyOf(values.keySet()), event);
  }

  private static OnheapIncrementalIndex buildIndex(ClusteredValueGroupsBaseTableProjectionSpec spec)
  {
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(DimensionsSpec.builder()
                                          .setDimensions(spec.getDimensionsSpec().getDimensions())
                                          .build())
        .withMetrics(new CountAggregatorFactory("count"))
        .withRollup(false)
        .withClusterSpec(spec)
        .build();
    return (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
  }

  @Test
  void testSingleColumnStringClusteringRoutesRowsByValue()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "acme", "region", "us-east-1")));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "region", "us-west-2")));
      index.add(row(T0 + 2, Map.of("tenant", "globex", "region", "eu-west-1")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(3, cbt.numRows());
      Assertions.assertEquals(2, cbt.getGroups().size());
      // Two distinct tenants → two groups with id 0 and id 1 in the shared string dict (insertion order).
      Assertions.assertEquals(List.of("acme", "globex"), cbt.getStringDictionary());

      final OnHeapClusterGroup acmeGroup = cbt.getGroups().get(List.of(0));
      final OnHeapClusterGroup globexGroup = cbt.getGroups().get(List.of(1));
      Assertions.assertNotNull(acmeGroup);
      Assertions.assertNotNull(globexGroup);
      Assertions.assertArrayEquals(new Object[]{"acme"}, acmeGroup.getClusteringValues());
      Assertions.assertArrayEquals(new Object[]{"globex"}, globexGroup.getClusteringValues());
      Assertions.assertEquals(2, acmeGroup.numRows());
      Assertions.assertEquals(1, globexGroup.numRows());
    }
  }

  @Test
  void testTwoColumnClusteringDistinguishesTuples()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"), new StringDimensionSchema("region"))
        .dimensions(new StringDimensionSchema("metric"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "acme", "region", "us-east-1", "metric", "page-views")));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "region", "us-east-1", "metric", "click")));
      index.add(row(T0 + 2, Map.of("tenant", "acme", "region", "us-west-2", "metric", "page-views")));
      index.add(row(T0 + 3, Map.of("tenant", "globex", "region", "us-east-1", "metric", "click")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(4, cbt.numRows());
      // Three distinct (tenant, region) tuples → three groups.
      Assertions.assertEquals(3, cbt.getGroups().size());
      // String dict: acme(0), us-east-1(1), us-west-2(2), globex(3) — insertion order.
      Assertions.assertEquals(List.of("acme", "us-east-1", "us-west-2", "globex"), cbt.getStringDictionary());

      final OnHeapClusterGroup acmeEast = cbt.getGroups().get(List.of(0, 1));
      final OnHeapClusterGroup acmeWest = cbt.getGroups().get(List.of(0, 2));
      final OnHeapClusterGroup globexEast = cbt.getGroups().get(List.of(3, 1));
      Assertions.assertNotNull(acmeEast);
      Assertions.assertNotNull(acmeWest);
      Assertions.assertNotNull(globexEast);
      Assertions.assertEquals(2, acmeEast.numRows());
      Assertions.assertEquals(1, acmeWest.numRows());
      Assertions.assertEquals(1, globexEast.numRows());
    }
  }

  @Test
  void testMixedTypeClusteringUsesPerTypeDictionaries()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"), new LongDimensionSchema("priority"))
        .dimensions(new StringDimensionSchema("metric"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "acme", "priority", 5L, "metric", "x")));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "priority", 10L, "metric", "y")));
      index.add(row(T0 + 2, Map.of("tenant", "globex", "priority", 5L, "metric", "z")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(3, cbt.getGroups().size());
      // STRING dict and LONG dict are independent — IDs index into the per-type dictionary at the column's position.
      Assertions.assertEquals(List.of("acme", "globex"), cbt.getStringDictionary());
      Assertions.assertEquals(List.of(5L, 10L), cbt.getLongDictionary());

      // (acme, 5) → (tenantId=0, priorityId=0); (acme, 10) → (0, 1); (globex, 5) → (1, 0).
      Assertions.assertNotNull(cbt.getGroups().get(List.of(0, 0)));
      Assertions.assertNotNull(cbt.getGroups().get(List.of(0, 1)));
      Assertions.assertNotNull(cbt.getGroups().get(List.of(1, 0)));
    }
  }

  @Test
  void testNullClusteringValueGetsItsOwnGroup()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("metric"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      final Map<String, Object> nullTenantRow = new HashMap<>();
      nullTenantRow.put("tenant", null);
      nullTenantRow.put("metric", "x");
      index.add(row(T0, nullTenantRow));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "metric", "y")));
      final Map<String, Object> nullTenantRow2 = new HashMap<>();
      nullTenantRow2.put("tenant", null);
      nullTenantRow2.put("metric", "z");
      index.add(row(T0 + 2, nullTenantRow2));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(3, cbt.numRows());
      Assertions.assertEquals(2, cbt.getGroups().size());

      // Null tenant lands in its own group; the entry in the string dict at position 0 is null (first-seen value).
      Assertions.assertEquals(Arrays.asList(null, "acme"), cbt.getStringDictionary());
      final OnHeapClusterGroup nullGroup = cbt.getGroups().get(List.of(0));
      final OnHeapClusterGroup acmeGroup = cbt.getGroups().get(List.of(1));
      Assertions.assertNotNull(nullGroup);
      Assertions.assertNotNull(acmeGroup);
      Assertions.assertArrayEquals(new Object[]{null}, nullGroup.getClusteringValues());
      Assertions.assertEquals(2, nullGroup.numRows());
      Assertions.assertEquals(1, acmeGroup.numRows());
    }
  }

  @Test
  void testGetMetadataExposesSortedClusteredBaseTableSchema()
  {
    // Ingest in an order that exercises both insertion-order vs sorted-order ID assignment AND null handling.
    // Strings are inserted as: globex, null, acme → sorted-nulls-first: [null, acme, globex] = ids [0, 1, 2].
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("metric"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "globex", "metric", "x")));
      final Map<String, Object> nullTenantRow = new HashMap<>();
      nullTenantRow.put("tenant", null);
      nullTenantRow.put("metric", "y");
      index.add(row(T0 + 1, nullTenantRow));
      index.add(row(T0 + 2, Map.of("tenant", "acme", "metric", "z")));
      index.add(row(T0 + 3, Map.of("tenant", "acme", "metric", "w")));

      final Metadata md = index.getMetadata();
      final ClusteredValueGroupsBaseTableSchema schema = md.getClusteredBaseTable();
      Assertions.assertNotNull(schema);
      // Sorted-nulls-first dictionary in the metadata, regardless of the insertion order.
      Assertions.assertEquals(Arrays.asList(null, "acme", "globex"), schema.getClusteringDictionaries().getStringDictionary());

      // Three groups: null tenant, acme, globex. clusteringValueIds reflect the sorted dictionary IDs.
      Assertions.assertEquals(3, schema.getClusterGroups().size());
      final Map<List<Integer>, Integer> rowCountByTuple = new HashMap<>();
      for (TableClusterGroupSpec group : schema.getClusterGroups()) {
        rowCountByTuple.put(group.getClusteringValueIds(), group.getNumRows());
      }
      Assertions.assertEquals(1, rowCountByTuple.get(List.of(0)).intValue()); // null tenant
      Assertions.assertEquals(2, rowCountByTuple.get(List.of(1)).intValue()); // acme
      Assertions.assertEquals(1, rowCountByTuple.get(List.of(2)).intValue()); // globex
    }
  }

  @Test
  void testIndexableAdapterExposesPerGroupView()
  {
    // Cluster on tenant; verify that each cluster group's IndexableAdapter exposes:
    //  - per-group dim names = non-clustering dims only (clustering is constant on the spec)
    //  - per-group row count
    //  - per-group ordering = segment ordering with clustering-prefix stripped
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .metrics(new CountAggregatorFactory("count"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "acme", "region", "us-east-1")));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "region", "us-west-2")));
      index.add(row(T0 + 2, Map.of("tenant", "globex", "region", "eu-west-1")));

      final IncrementalIndexAdapter adapter = new IncrementalIndexAdapter(
          Intervals.of("2026-01-01/2026-01-02"),
          index,
          new RoaringBitmapFactory()
      );
      final List<TableClusterGroupSpec> groupSpecs = adapter.getMetadata().getClusteredBaseTable().getClusterGroups();
      Assertions.assertEquals(2, groupSpecs.size());

      // Find the acme spec (clustering value "acme")
      TableClusterGroupSpec acmeSpec = null;
      TableClusterGroupSpec globexSpec = null;
      for (TableClusterGroupSpec gs : groupSpecs) {
        if ("acme".equals(gs.lookupClusteringValues()[0])) {
          acmeSpec = gs;
        } else if ("globex".equals(gs.lookupClusteringValues()[0])) {
          globexSpec = gs;
        }
      }
      Assertions.assertNotNull(acmeSpec);
      Assertions.assertNotNull(globexSpec);

      final IndexableAdapter acmeAdapter = adapter.getClusterGroupAdapter(acmeSpec);
      Assertions.assertEquals(2, acmeAdapter.getNumRows());
      Assertions.assertEquals(List.of("region"), acmeAdapter.getDimensionNames(false));
      Assertions.assertEquals(List.of("count"), acmeAdapter.getMetricNames());

      final IndexableAdapter globexAdapter = adapter.getClusterGroupAdapter(globexSpec);
      Assertions.assertEquals(1, globexAdapter.getNumRows());
      Assertions.assertEquals(List.of("region"), globexAdapter.getDimensionNames(false));
    }
  }

  @Test
  void testClusteringVirtualColumnRoutesViaExpression()
  {
    // Cluster on tenant_lower := lower(tenant). Ingest "Acme" and "ACME" — both should land in the same group
    // after lowercase normalization.
    final ExpressionVirtualColumn lowerTenant = new ExpressionVirtualColumn(
        "tenant_lower",
        "lower(tenant)",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    );
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(lowerTenant))
        .clusteringColumns(new StringDimensionSchema("tenant_lower"))
        .dimensions(new StringDimensionSchema("tenant"), new StringDimensionSchema("metric"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "Acme", "metric", "x")));
      index.add(row(T0 + 1, Map.of("tenant", "ACME", "metric", "y")));
      index.add(row(T0 + 2, Map.of("tenant", "globex", "metric", "z")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(3, cbt.numRows());
      // Acme + ACME both map to lowered "acme" → one group; "globex" lowers to itself → second group.
      Assertions.assertEquals(2, cbt.getGroups().size());
      Assertions.assertEquals(List.of("acme", "globex"), cbt.getStringDictionary());

      final OnHeapClusterGroup acmeGroup = cbt.getGroups().get(List.of(0));
      final OnHeapClusterGroup globexGroup = cbt.getGroups().get(List.of(1));
      Assertions.assertNotNull(acmeGroup);
      Assertions.assertNotNull(globexGroup);
      Assertions.assertEquals(2, acmeGroup.numRows());
      Assertions.assertEquals(1, globexGroup.numRows());
    }
  }

  @Test
  void testGetLastRowIndexIsZeroBasedLastIndexNotRowCount()
  {
    // getLastRowIndex() must return the last assigned 0-based row index (numRows - 1), matching
    // OnheapIncrementalIndex; returning the row count would let a row appended after a cursor snapshots this
    // value leak into that in-flight query.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("tenant", "acme", "region", "us-east-1")));
      index.add(row(T0 + 1, Map.of("tenant", "acme", "region", "us-west-2")));
      index.add(row(T0 + 2, Map.of("tenant", "globex", "region", "eu-west-1")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      final OnHeapClusterGroup acmeGroup = cbt.getGroups().get(List.of(0));
      final OnHeapClusterGroup globexGroup = cbt.getGroups().get(List.of(1));
      Assertions.assertEquals(2, acmeGroup.numRows());
      Assertions.assertEquals(1, acmeGroup.getLastRowIndex());
      Assertions.assertEquals(1, globexGroup.numRows());
      Assertions.assertEquals(0, globexGroup.getLastRowIndex());
    }
  }

  @Test
  void testConcurrentIngestAndSnapshotIsThreadSafe() throws InterruptedException
  {
    // IncrementalIndex's contract: the cursor-factory/snapshot methods may run concurrently with single-threaded
    // add(). The query path snapshots the segment-wide groups + clustering dictionaries (toMetadataSchema iterates
    // groups and sorts each dict; getStringDictionary walks a dict). With the thread-safe DimensionDictionary +
    // ConcurrentHashMap these must not throw or tear; the previous plain HashMap + ArrayList could throw
    // ConcurrentModificationException or read a half-resized structure.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .build();
    final int numTenants = 4000;
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean writerDone = new AtomicBoolean(false);

      final Thread writer = new Thread(() -> {
        try {
          for (int i = 0; i < numTenants; i++) {
            index.add(row(T0 + i, Map.of("tenant", "tenant-" + i, "region", "us-east-1")));
          }
        }
        catch (Throwable t) {
          failure.compareAndSet(null, t);
        }
        finally {
          writerDone.set(true);
        }
      });

      final Runnable reader = () -> {
        try {
          while (!writerDone.get()) {
            final ClusteredValueGroupsBaseTableSchema snapshot = cbt.toMetadataSchema();
            Assertions.assertNotNull(snapshot);
            cbt.getStringDictionary();
          }
        }
        catch (Throwable t) {
          failure.compareAndSet(null, t);
        }
      };
      final Thread reader1 = new Thread(reader);
      final Thread reader2 = new Thread(reader);

      writer.start();
      reader1.start();
      reader2.start();
      writer.join();
      reader1.join();
      reader2.join();

      if (failure.get() != null) {
        throw new AssertionError("concurrent ingest + snapshot threw", failure.get());
      }
      Assertions.assertEquals(numTenants, cbt.getGroups().size());
      Assertions.assertEquals(numTenants, cbt.numRows());
    }
  }

  @Test
  void testNumericClusteringColumnsAcceptStringValues()
  {
    // CSV/TSV (and string-typed JSON) deliver clustering values as Strings; numeric clustering columns must parse
    // them, not throw. Cluster on a DOUBLE and a FLOAT column whose row values arrive as Strings.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new DoubleDimensionSchema("price"), new FloatDimensionSchema("rate"))
        .dimensions(new StringDimensionSchema("region"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      index.add(row(T0, Map.of("price", "9.99", "rate", "0.5", "region", "us-east-1")));
      index.add(row(T0 + 1, Map.of("price", "9.99", "rate", "0.5", "region", "us-west-2")));
      index.add(row(T0 + 2, Map.of("price", "1.5", "rate", "0.25", "region", "eu-west-1")));

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertNotNull(cbt);
      Assertions.assertEquals(3, cbt.numRows());
      // Two distinct (price, rate) tuples → two groups; String inputs coerced to the declared numeric types.
      Assertions.assertEquals(2, cbt.getGroups().size());
      final OnHeapClusterGroup firstGroup = cbt.getGroups().get(List.of(0, 0));
      Assertions.assertNotNull(firstGroup);
      Assertions.assertArrayEquals(new Object[]{9.99d, 0.5f}, firstGroup.getClusteringValues());
      Assertions.assertEquals(2, firstGroup.numRows());
    }
  }

  @Test
  void testUnparseableNumericClusteringValueIsRecoverableNotFatal()
  {
    // A genuinely non-numeric value for a LONG clustering column is bad row data, not a Druid bug: it must surface
    // as a recoverable ParseException (governed by maxParseExceptions) with the row still ingested into the null
    // group — not abort the whole task with a fatal/defensive error.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new LongDimensionSchema("tenantId"))
        .dimensions(new StringDimensionSchema("region"))
        .build();
    try (OnheapIncrementalIndex index = buildIndex(spec)) {
      final IncrementalIndexAddResult result =
          index.add(row(T0, Map.of("tenantId", "not-a-number", "region", "us-east-1")));

      Assertions.assertNotNull(result.getParseException(), "expected a recoverable parse exception");
      Assertions.assertTrue(
          result.getParseException().getMessage().contains("tenantId"),
          "parse exception should name the offending column, got: " + result.getParseException().getMessage()
      );

      final OnHeapClusteredBaseTable cbt = index.getClusteredBaseTable();
      Assertions.assertEquals(1, cbt.numRows());
      Assertions.assertEquals(1, cbt.getGroups().size());
      // Unparseable LONG → null → routed to the null-keyed group.
      Assertions.assertEquals(Arrays.asList((Long) null), cbt.getLongDictionary());
      Assertions.assertArrayEquals(
          new Object[]{null},
          cbt.getGroups().values().iterator().next().getClusteringValues()
      );
    }
  }
}
