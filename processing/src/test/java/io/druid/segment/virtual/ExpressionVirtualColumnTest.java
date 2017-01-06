/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.virtual;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionVirtualColumnTest
{
  @Test
  public void testMakeSelectors()
  {
    final TestColumnSelectorFactory columnSelectorFactory = new TestColumnSelectorFactory();
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn("expr", "x + y");

    final ObjectColumnSelector objectSelector = virtualColumn.makeObjectColumnSelector("expr", columnSelectorFactory);
    final DimensionSelector dimensionSelector = virtualColumn.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "x"),
        columnSelectorFactory
    );
    final FloatColumnSelector floatSelector = virtualColumn.makeFloatColumnSelector("expr", columnSelectorFactory);
    final LongColumnSelector longSelector = virtualColumn.makeLongColumnSelector("expr", columnSelectorFactory);

    columnSelectorFactory.setRow(
        new MapBasedInputRow(
            0,
            ImmutableList.<String>of(),
            ImmutableMap.<String, Object>of()
        )
    );

    Assert.assertEquals(null, objectSelector.get());
    Assert.assertEquals(null, dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(0.0f, floatSelector.get(), 0.0f);
    Assert.assertEquals(0L, longSelector.get());

    columnSelectorFactory.setRow(
        new MapBasedInputRow(
            0,
            ImmutableList.<String>of(),
            ImmutableMap.<String, Object>of("x", 4)
        )
    );

    Assert.assertEquals(null, objectSelector.get());
    Assert.assertEquals(null, dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(0.0f, floatSelector.get(), 0.0f);
    Assert.assertEquals(0L, longSelector.get());

    columnSelectorFactory.setRow(
        new MapBasedInputRow(
            0,
            ImmutableList.<String>of(),
            ImmutableMap.<String, Object>of("x", 2.1, "y", 3L)
        )
    );

    Assert.assertEquals(5.1d, objectSelector.get());
    Assert.assertEquals("5.1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(5.1f, floatSelector.get(), 0.0f);
    Assert.assertEquals(5L, longSelector.get());
  }

  @Test
  public void testRequiredColumns()
  {
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn("expr", "x + y");
    Assert.assertEquals(ImmutableList.of("x", "y"), virtualColumn.requiredColumns());
  }
}
