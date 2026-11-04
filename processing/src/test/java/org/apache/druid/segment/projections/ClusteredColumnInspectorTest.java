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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ClusteredColumnInspectorTest
{
  private static final RowSignature CLUSTERING = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  /**
   * Builds a summary over groups for tenants {@code a}, {@code b}, {@code c}, in that order, with the given
   * non-clustering columns after {@code __time}.
   */
  private static ClusteredValueGroupsBaseTableSchema summary(String... columns)
  {
    final ClusterGroupSchemaTestHelpers.Built built = ClusterGroupSchemaTestHelpers.buildClusterGroups(
        CLUSTERING,
        List.of(List.of("a"), List.of("b"), List.of("c"))
    );
    final List<String> allColumns = new ArrayList<>(List.of("tenant", ColumnHolder.TIME_COLUMN_NAME));
    allColumns.addAll(List.of(columns));
    return new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        allColumns,
        List.of(OrderBy.ascending("tenant"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        CLUSTERING,
        null,
        built.dictionaries(),
        built.specs()
    );
  }

  /**
   * Returns the descriptor key of {@code column} in the cluster group at {@code groupIndex}.
   */
  private static String fileName(ClusteredValueGroupsBaseTableSchema summary, int groupIndex, String column)
  {
    return Projections.getClusterGroupSegmentInternalFileName(
        summary.getClusterGroups().get(groupIndex).getClusteringValueIds(),
        column
    );
  }

  private static ColumnDescriptor descriptor(ValueType valueType, boolean hasMultipleValues)
  {
    return new ColumnDescriptor(valueType, hasMultipleValues, List.of());
  }

  private static ColumnDescriptor typed(ColumnType type)
  {
    return new ColumnDescriptor(type.getType(), false, List.of(new TypedPartSerde(type)));
  }

  @Test
  void testClusteringColumnFromSummary()
  {
    final ClusteredColumnInspector capabilities = new ClusteredColumnInspector(summary(), Map.of());
    final ColumnCapabilities tenant = capabilities.getColumnCapabilities("tenant");
    Assertions.assertEquals(ColumnType.STRING, tenant.toColumnType());
    Assertions.assertTrue(tenant.hasMultipleValues().isFalse());
  }

  @Test
  void testUnknownColumnIsNull()
  {
    final ClusteredValueGroupsBaseTableSchema summary = summary("x");
    final ClusteredColumnInspector capabilities = new ClusteredColumnInspector(
        summary,
        Map.of(fileName(summary, 0, "x"), descriptor(ValueType.LONG, false))
    );
    Assertions.assertNull(capabilities.getColumnCapabilities("nope"));
  }

  @Test
  void testMultipleValuesInAnyGroup()
  {
    final ClusteredValueGroupsBaseTableSchema summary = summary("tags");
    final Map<String, ColumnDescriptor> descriptors = new HashMap<>();
    descriptors.put(fileName(summary, 0, "tags"), descriptor(ValueType.STRING, false));
    descriptors.put(fileName(summary, 1, "tags"), descriptor(ValueType.STRING, true));
    descriptors.put(fileName(summary, 2, "tags"), descriptor(ValueType.STRING, false));

    final ColumnCapabilities tags = new ClusteredColumnInspector(summary, descriptors).getColumnCapabilities("tags");
    Assertions.assertEquals(ColumnType.STRING, tags.toColumnType());
    Assertions.assertTrue(tags.hasMultipleValues().isTrue());
  }

  @Test
  void testTypeIsLeastRestrictiveAcrossGroups()
  {
    final ClusteredValueGroupsBaseTableSchema summary = summary("num");
    final Map<String, ColumnDescriptor> descriptors = new HashMap<>();
    descriptors.put(fileName(summary, 0, "num"), descriptor(ValueType.LONG, false));
    descriptors.put(fileName(summary, 1, "num"), descriptor(ValueType.LONG, false));
    descriptors.put(fileName(summary, 2, "num"), descriptor(ValueType.DOUBLE, false));

    final ColumnCapabilities num = new ClusteredColumnInspector(summary, descriptors).getColumnCapabilities("num");
    Assertions.assertEquals(ColumnType.DOUBLE, num.toColumnType());
    Assertions.assertTrue(num.hasMultipleValues().isFalse());
  }

  @Test
  void testColumnMissingFromFirstGroup()
  {
    final ClusteredValueGroupsBaseTableSchema summary = summary("sparse");
    final ClusteredColumnInspector capabilities = new ClusteredColumnInspector(
        summary,
        Map.of(fileName(summary, 1, "sparse"), descriptor(ValueType.FLOAT, false))
    );
    Assertions.assertEquals(ColumnType.FLOAT, capabilities.getColumnCapabilities("sparse").toColumnType());
  }

  @Test
  void testNullsUnknownExceptForTime()
  {
    // descriptors don't record nulls, so the inspector must not claim a column has none
    final ClusteredValueGroupsBaseTableSchema summary = summary("num");
    final Map<String, ColumnDescriptor> descriptors = new HashMap<>();
    descriptors.put(fileName(summary, 0, ColumnHolder.TIME_COLUMN_NAME), descriptor(ValueType.LONG, false));
    descriptors.put(fileName(summary, 1, ColumnHolder.TIME_COLUMN_NAME), descriptor(ValueType.LONG, false));
    descriptors.put(fileName(summary, 0, "num"), descriptor(ValueType.LONG, false));
    descriptors.put(fileName(summary, 1, "num"), descriptor(ValueType.LONG, false));

    final ClusteredColumnInspector capabilities = new ClusteredColumnInspector(summary, descriptors);
    Assertions.assertTrue(capabilities.getColumnCapabilities("num").hasNulls().isUnknown());
    Assertions.assertTrue(capabilities.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME).hasNulls().isFalse());
  }

  @Test
  void testCombineGroupCapabilities()
  {
    final ColumnCapabilities combined = ClusteredColumnInspector.combineGroupCapabilities(
        "num",
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG).setHasNulls(false),
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE).setHasNulls(true)
    );
    Assertions.assertEquals(ColumnType.DOUBLE, combined.toColumnType());
    Assertions.assertTrue(combined.hasNulls().isTrue());
    Assertions.assertTrue(combined.hasMultipleValues().isFalse());

    // dictionary flags never survive, since each group has its own dictionary
    final ColumnCapabilities strings = ClusteredColumnInspector.combineGroupCapabilities(
        "tags",
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                    .setDictionaryEncoded(true)
                                    .setDictionaryValuesUnique(true)
                                    .setHasMultipleValues(true)
                                    .setHasNulls(false),
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                    .setDictionaryEncoded(true)
                                    .setDictionaryValuesUnique(true)
                                    .setHasMultipleValues(false)
    );
    Assertions.assertEquals(ColumnType.STRING, strings.toColumnType());
    Assertions.assertTrue(strings.hasMultipleValues().isTrue());
    Assertions.assertTrue(strings.hasNulls().isUnknown());
    Assertions.assertTrue(strings.isDictionaryEncoded().isFalse());
    Assertions.assertTrue(strings.areDictionaryValuesUnique().isFalse());
  }

  @Test
  void testIncompatibleTypesAcrossGroups()
  {
    final ClusteredValueGroupsBaseTableSchema summary = summary("bad");
    final Map<String, ColumnDescriptor> descriptors = new HashMap<>();
    descriptors.put(fileName(summary, 0, "bad"), typed(ColumnType.ofComplex("foo")));
    descriptors.put(fileName(summary, 1, "bad"), typed(ColumnType.ofComplex("bar")));

    final ClusteredColumnInspector capabilities = new ClusteredColumnInspector(summary, descriptors);
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> capabilities.getColumnCapabilities("bad")
    );
    Assertions.assertEquals("Column[bad] has incompatible types in different cluster groups", e.getMessage());
  }

  /**
   * Part serde that only reports a type, for descriptors whose type a bare {@link ValueType} can't express.
   */
  private static final class TypedPartSerde implements ColumnPartSerde
  {
    private final ColumnType type;

    private TypedPartSerde(ColumnType type)
    {
      this.type = type;
    }

    @Override
    public Serializer getSerializer()
    {
      return null;
    }

    @Override
    public Deserializer getDeserializer()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnType getColumnType()
    {
      return type;
    }
  }
}
