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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumnBitmapDisablingIndexSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.serde.StringUtf8ColumnIndexSupplier;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class VirtualColumnBitmapDisablingIndexSelectorTest extends InitializedNullHandlingTest
{
  @Test
  public void testNative_disablesIndexSupplierForVirtualColumnsOnly() throws Exception
  {
    final QueryableIndex index = TestIndex.getMMappedWikipediaIndex();

    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(new ExpressionVirtualColumn(
                                                                                     "v0",
                                                                                     "countryName",
                                                                                     ColumnType.STRING,
                                                                                     ExprMacroTable.nil()
                                                                                 ),
                                                                                 new ExpressionVirtualColumn("v1",
                                                                                                             "cityName",
                                                                                                             ColumnType.STRING,
                                                                                                             ExprMacroTable.nil()
                                                                                 )
    ));

    try (Closer closer = Closer.create()) {
      final ColumnCache columnCache = new ColumnCache(index, virtualColumns, closer);

      assertEquals(StringUtf8ColumnIndexSupplier.class,
                   Objects.requireNonNull(columnCache.getIndexSupplier("v0")).getClass()
      );
      assertEquals(StringUtf8ColumnIndexSupplier.class,
                   Objects.requireNonNull(columnCache.getIndexSupplier("v1")).getClass()
      );

      final ColumnIndexSelector vcIndexDisabled = new VirtualColumnBitmapDisablingIndexSelector(columnCache,
                                                                                                virtualColumns
      );

      assertSame(NoIndexesColumnIndexSupplier.getInstance(), vcIndexDisabled.getIndexSupplier("v0"));
      assertSame(NoIndexesColumnIndexSupplier.getInstance(), vcIndexDisabled.getIndexSupplier("v1"));

      assertEquals(
          StringUtf8ColumnIndexSupplier.class,
          Objects.requireNonNull(vcIndexDisabled.getIndexSupplier("countryName")).getClass()
      );
      assertEquals(
          StringUtf8ColumnIndexSupplier.class,
          Objects.requireNonNull(vcIndexDisabled.getIndexSupplier("cityName")).getClass()
      );
    }
  }
}
