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

package org.apache.druid.segment.join.table;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.lookup.RetainedLookupTestHelper;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;

public class IndexedTableColumnSelectorFactoryTest extends InitializedNullHandlingTest
{
  @ParameterizedTest
  @CsvSource({"dim, value", "absent, missing"})
  public void testLookupReleasedWithSelectorResources(final String dimension, final String expected) throws IOException
  {
    final RetainedLookupTestHelper lookup = new RetainedLookupTestHelper();
    final IndexedTable table = Mockito.mock(IndexedTable.class);
    final IndexedTable.Reader reader = Mockito.mock(IndexedTable.Reader.class);
    Mockito.when(table.rowSignature()).thenReturn(RowSignature.builder().add("dim", ColumnType.STRING).build());
    Mockito.when(table.numRows()).thenReturn(1);
    Mockito.when(table.columnReader(0)).thenReturn(reader);
    Mockito.when(reader.read(0)).thenReturn("key");

    try (final Closer closer = Closer.create()) {
      final IndexedTableColumnSelectorFactory factory = new IndexedTableColumnSelectorFactory(table, () -> 0, closer);
      final DimensionSelector selector = factory.makeDimensionSelector(lookup.dimensionSpec(dimension, "alias"));
      for (int i = 0; i < 3; i++) {
        Assertions.assertEquals(expected, selector.lookupName(selector.getRow().get(0)));
      }
      Assertions.assertEquals(1, lookup.getAcquisitions());
      Assertions.assertEquals(0, lookup.getReleases());
    }
    Assertions.assertEquals(1, lookup.getReleases());
    if ("dim".equals(dimension)) {
      Mockito.verify(reader).close();
    }
  }
}
