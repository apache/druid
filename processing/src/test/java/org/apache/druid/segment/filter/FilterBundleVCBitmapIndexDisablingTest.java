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
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterBundle;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumnBitmapDisablingIndexSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FilterBundleVCBitmapIndexDisablingTest extends InitializedNullHandlingTest
{

  @Test
  public void testVCBitmapIndexEnabled()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(
        new ExpressionVirtualColumn("v1",
                                    "countryName", ColumnType.STRING,
                                    ExprMacroTable.nil()
        )
    ));
    final QueryableIndex index = TestIndex.getMMappedWikipediaIndex();
    BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

    try (Closer closer = Closer.create()) {
      ColumnCache columnCache = new ColumnCache(index, virtualColumns, closer);
      VirtualColumnBitmapDisablingIndexSelector selector = new VirtualColumnBitmapDisablingIndexSelector(columnCache,
                                                                                                         virtualColumns
      );

      final FilterBundle filterBundleIndexEnabled = makeFilterBundle(new AndFilter(ImmutableList.of(
          new EqualityFilter("v1", ColumnType.STRING, "United States", null),
          new EqualityFilter("countryName",
                             ColumnType.STRING,
                             "United States",
                             null
          )
      )), columnCache, bitmapFactory);

      final FilterBundle filterBundleIndexDisabled = makeFilterBundle(new AndFilter(ImmutableList.of(
          new EqualityFilter("v1", ColumnType.STRING, "United States", null),
          new EqualityFilter("countryName",
                             ColumnType.STRING,
                             "United States",
                             null
          )
      )), selector, bitmapFactory);

      Assert.assertEquals(2, filterBundleIndexEnabled.getIndex().getIndexInfo().getIndexes().size());
      Assert.assertEquals("v1 = United States", filterBundleIndexEnabled.getIndex().getIndexInfo().getIndexes().get(0).getFilter());
      Assert.assertEquals("countryName = United States", filterBundleIndexEnabled.getIndex().getIndexInfo().getIndexes().get(1).getFilter());

      Assert.assertNull(filterBundleIndexDisabled.getIndex().getIndexInfo().getIndexes());
      Assert.assertEquals("countryName = United States", filterBundleIndexDisabled.getIndex().getIndexInfo().getFilter());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  protected FilterBundle makeFilterBundle(
      final Filter filter,
      ColumnIndexSelector selector,
      BitmapFactory bitmapFactory
  )
  {
    return new FilterBundle.Builder(filter, selector, false).build(new DefaultBitmapResultFactory(bitmapFactory),
                                                                   selector.getNumRows(),
                                                                   selector.getNumRows(),
                                                                   false
    );
  }
}
