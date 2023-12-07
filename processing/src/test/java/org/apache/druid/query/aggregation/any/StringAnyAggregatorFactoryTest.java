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

package org.apache.druid.query.aggregation.any;

import com.google.common.collect.Lists;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.vector.TestVectorColumnSelectorFactory;
import org.apache.druid.segment.virtual.FallbackVirtualColumnTest;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class StringAnyAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final int MAX_STRING_BYTES = 10;

  private TestColumnSelectorFactory columnInspector;
  private ColumnCapabilities capabilities;
  private TestVectorColumnSelectorFactory vectorSelectorFactory;
  private StringAnyAggregatorFactory target;

  @Before
  public void setUp()
  {
    target = new StringAnyAggregatorFactory(NAME, FIELD_NAME, MAX_STRING_BYTES, true);
    columnInspector = new TestColumnSelectorFactory();
    vectorSelectorFactory = new TestVectorColumnSelectorFactory();
    capabilities = ColumnCapabilitiesImpl.createDefault().setHasMultipleValues(true);
    vectorSelectorFactory.addCapabilities(FIELD_NAME, capabilities);
    vectorSelectorFactory.addMVDVS(FIELD_NAME, new FallbackVirtualColumnTest.SameMultiVectorSelector());
  }

  @Test
  public void canVectorizeWithoutCapabilitiesShouldReturnTrue()
  {
    Assert.assertTrue(target.canVectorize(columnInspector));
  }

  @Test
  public void factorizeVectorWithoutCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithUnknownCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithMultipleValuesCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithoutMultipleValuesCapabilitiesShouldReturnAggregatorWithSingleDimensionSelector()
  {
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void testFactorize()
  {
    Aggregator res = target.factorize(new TestColumnSelectorFactory());
    Assert.assertTrue(res instanceof StringAnyAggregator);
    res.aggregate();
    Assert.assertEquals(null, res.get());
    StringAnyVectorAggregator vectorAggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertTrue(vectorAggregator.isAggregateMultipleValues());
  }

  @Test
  public void testSvdStringAnyAggregator()
  {
    TestColumnSelectorFactory columnSelectorFactory = new TestColumnSelectorFactory();
    Aggregator res = target.factorize(columnSelectorFactory);
    Assert.assertTrue(res instanceof StringAnyAggregator);
    columnSelectorFactory.moveSelectorCursorToNext();
    res.aggregate();
    Assert.assertEquals("CCCC", res.get());
  }

  @Test
  public void testMvdStringAnyAggregator()
  {
    TestColumnSelectorFactory columnSelectorFactory = new TestColumnSelectorFactory();
    Aggregator res = target.factorize(columnSelectorFactory);
    Assert.assertTrue(res instanceof StringAnyAggregator);
    columnSelectorFactory.moveSelectorCursorToNext();
    columnSelectorFactory.moveSelectorCursorToNext();
    res.aggregate();
    Assert.assertEquals("[AAAA, AAA", res.get());
  }

  @Test
  public void testMvdStringAnyAggregatorWithAggregateMultipleToFalse()
  {
    StringAnyAggregatorFactory target = new StringAnyAggregatorFactory(NAME, FIELD_NAME, MAX_STRING_BYTES, false);
    TestColumnSelectorFactory columnSelectorFactory = new TestColumnSelectorFactory();
    Aggregator res = target.factorize(columnSelectorFactory);
    Assert.assertTrue(res instanceof StringAnyAggregator);
    columnSelectorFactory.moveSelectorCursorToNext();
    columnSelectorFactory.moveSelectorCursorToNext();
    res.aggregate();
    // picks up first value in mvd list
    Assert.assertEquals("AAAA", res.get());
  }

  static class TestColumnSelectorFactory implements ColumnSelectorFactory
  {
    List<String> mvd = Lists.newArrayList("AAAA", "AAAAB", "AAAC");
    final Object[] mvds = {null, "CCCC", mvd, "BBBB", "EEEE"};
    Integer maxStringBytes = 1024;
    TestObjectColumnSelector<Object> objectColumnSelector = new TestObjectColumnSelector<>(mvds);

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return objectColumnSelector;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return ColumnCapabilitiesImpl.createDefault().setHasMultipleValues(true);
    }

    public void moveSelectorCursorToNext()
    {
      objectColumnSelector.increment();
    }
  }
}
