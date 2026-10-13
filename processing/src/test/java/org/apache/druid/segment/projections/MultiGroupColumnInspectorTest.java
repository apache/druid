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

import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MultiGroupColumnInspectorTest extends InitializedNullHandlingTest
{
  private static final RowSignature CLUSTERING = RowSignature.builder()
                                                             .add("tenant", ColumnType.STRING)
                                                             .add("priority", ColumnType.LONG)
                                                             .build();

  /**
   * Covers every group: {@code tags} is a dictionary-encoded string with multiple values and nulls, and the
   * clustering columns have their declared types.
   */
  private static final ColumnInspector ALL_GROUPS = column -> switch (column) {
    case "tenant" -> ClusteredColumnInspector.clusteringColumnCapabilities(ColumnType.STRING);
    case "priority" -> ClusteredColumnInspector.clusteringColumnCapabilities(ColumnType.LONG);
    case "tags" -> new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                               .setDictionaryEncoded(true)
                                               .setDictionaryValuesSorted(true)
                                               .setDictionaryValuesUnique(true)
                                               .setHasBitmapIndexes(true)
                                               .setHasMultipleValues(true)
                                               .setHasNulls(true);
    default -> null;
  };

  @Test
  void testClusteringColumnsAreConstants()
  {
    final MultiGroupColumnInspector inspector =
        new MultiGroupColumnInspector(CLUSTERING, VirtualColumns.EMPTY, ALL_GROUPS);

    final ColumnCapabilities tenant = inspector.getColumnCapabilities("tenant");
    Assertions.assertEquals(ColumnType.STRING, tenant.toColumnType());
    Assertions.assertTrue(tenant.hasMultipleValues().isFalse());
    Assertions.assertTrue(tenant.isDictionaryEncoded().isFalse());
    Assertions.assertEquals(ColumnType.LONG, inspector.getColumnCapabilities("priority").toColumnType());
  }

  @Test
  void testOtherColumnsComeFromAllGroupsWithoutDictionary()
  {
    final MultiGroupColumnInspector inspector =
        new MultiGroupColumnInspector(CLUSTERING, VirtualColumns.EMPTY, ALL_GROUPS);

    final ColumnCapabilities tags = inspector.getColumnCapabilities("tags");
    Assertions.assertEquals(ColumnType.STRING, tags.toColumnType());
    Assertions.assertTrue(tags.hasMultipleValues().isTrue());
    Assertions.assertTrue(tags.hasNulls().isTrue());
    Assertions.assertTrue(tags.isDictionaryEncoded().isFalse());
    Assertions.assertTrue(tags.areDictionaryValuesSorted().isFalse());
    Assertions.assertTrue(tags.areDictionaryValuesUnique().isFalse());
    Assertions.assertFalse(tags.hasBitmapIndexes());
    Assertions.assertNull(inspector.getColumnCapabilities("nonexistent"));
  }

  @Test
  void testQueryVirtualColumns()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        // shadows a clustering column, so it is not a constant
        new ExpressionVirtualColumn("tenant", "strlen(\"tags\")", ColumnType.LONG, TestExprMacroTable.INSTANCE),
        // reads a column that has multiple values in some group
        new ExpressionVirtualColumn("v0", "concat(\"tags\", 'x')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final MultiGroupColumnInspector inspector = new MultiGroupColumnInspector(CLUSTERING, virtualColumns, ALL_GROUPS);

    Assertions.assertEquals(ColumnType.LONG, inspector.getColumnCapabilities("tenant").toColumnType());
    final ColumnCapabilities v0 = inspector.getColumnCapabilities("v0");
    Assertions.assertEquals(ColumnType.STRING, v0.toColumnType());
    Assertions.assertTrue(v0.hasMultipleValues().isTrue());
    Assertions.assertTrue(v0.isDictionaryEncoded().isFalse());
  }
}
