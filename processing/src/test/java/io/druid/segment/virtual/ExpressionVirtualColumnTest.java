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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.BucketExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionVirtualColumnTest
{
  private static final InputRow ROW0 = new MapBasedInputRow(
      0,
      ImmutableList.<String>of(),
      ImmutableMap.<String, Object>of()
  );

  private static final InputRow ROW1 = new MapBasedInputRow(
      0,
      ImmutableList.<String>of(),
      ImmutableMap.<String, Object>of("x", 4)
  );

  private static final InputRow ROW2 = new MapBasedInputRow(
      0,
      ImmutableList.<String>of(),
      ImmutableMap.<String, Object>of("x", 2.1, "y", 3L)
  );

  private static final ExpressionVirtualColumn XPLUSY = new ExpressionVirtualColumn("expr", "x + y");
  private static final TestColumnSelectorFactory COLUMN_SELECTOR_FACTORY = new TestColumnSelectorFactory();

  @Test
  public void testObjectSelector()
  {
    final ObjectColumnSelector selector = XPLUSY.makeObjectColumnSelector("expr", COLUMN_SELECTOR_FACTORY);

    COLUMN_SELECTOR_FACTORY.setRow(ROW0);
    Assert.assertEquals(null, selector.get());

    COLUMN_SELECTOR_FACTORY.setRow(ROW1);
    Assert.assertEquals(null, selector.get());

    COLUMN_SELECTOR_FACTORY.setRow(ROW2);
    Assert.assertEquals(5.1d, selector.get());
  }

  @Test
  public void testLongSelector()
  {
    final LongColumnSelector selector = XPLUSY.makeLongColumnSelector("expr", COLUMN_SELECTOR_FACTORY);

    COLUMN_SELECTOR_FACTORY.setRow(ROW0);
    Assert.assertEquals(0L, selector.get());

    COLUMN_SELECTOR_FACTORY.setRow(ROW1);
    Assert.assertEquals(0L, selector.get());

    COLUMN_SELECTOR_FACTORY.setRow(ROW2);
    Assert.assertEquals(5L, selector.get());
  }

  @Test
  public void testFloatSelector()
  {
    final FloatColumnSelector selector = XPLUSY.makeFloatColumnSelector("expr", COLUMN_SELECTOR_FACTORY);

    COLUMN_SELECTOR_FACTORY.setRow(ROW0);
    Assert.assertEquals(0.0f, selector.get(), 0.0f);

    COLUMN_SELECTOR_FACTORY.setRow(ROW1);
    Assert.assertEquals(0.0f, selector.get(), 0.0f);

    COLUMN_SELECTOR_FACTORY.setRow(ROW2);
    Assert.assertEquals(5.1f, selector.get(), 0.0f);
  }

  @Test
  public void testDimensionSelector()
  {
    final DimensionSelector selector = XPLUSY.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "x"),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.<String>notNull());

    COLUMN_SELECTOR_FACTORY.setRow(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    COLUMN_SELECTOR_FACTORY.setRow(ROW1);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    COLUMN_SELECTOR_FACTORY.setRow(ROW2);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5.1", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testDimensionSelectorWithExtraction()
  {
    final DimensionSelector selector = XPLUSY.makeDimensionSelector(
        new ExtractionDimensionSpec("expr", "x", new BucketExtractionFn(1.0, 0.0)),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.<String>notNull());

    COLUMN_SELECTOR_FACTORY.setRow(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    COLUMN_SELECTOR_FACTORY.setRow(ROW1);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    COLUMN_SELECTOR_FACTORY.setRow(ROW2);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(true, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testRequiredColumns()
  {
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn("expr", "x + y");
    Assert.assertEquals(ImmutableList.of("x", "y"), virtualColumn.requiredColumns());
  }
}
