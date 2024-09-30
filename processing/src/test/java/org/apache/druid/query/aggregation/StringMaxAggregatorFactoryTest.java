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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Comparator;

public class StringMaxAggregatorFactoryTest
{
  private final ExprMacroTable macroTable = ExprMacroTable.nil();

  @Test
  public void testBuildAggregators()
  {
    BaseObjectColumnValueSelector<String> selector = Mockito.mock(BaseObjectColumnValueSelector.class);
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);

    Assert.assertEquals(factory, new StringMaxAggregatorFactory("name", "fieldName", null, true, null, macroTable));
    Aggregator aggregator = factory.buildAggregator(selector);

    Assert.assertNotNull(aggregator);
    Assert.assertEquals(StringMaxAggregator.class, aggregator.getClass());

    BufferAggregator bufferAggregator = factory.buildBufferAggregator(selector);

    Assert.assertNotNull(bufferAggregator);
    Assert.assertEquals(StringMaxBufferAggregator.class, bufferAggregator.getClass());
  }

  @Test
  public void testFactorizeVector()
  {
    VectorColumnSelectorFactory columnSelectorFactory = Mockito.mock(VectorColumnSelectorFactory.class);
    VectorValueSelector selector = Mockito.mock(VectorValueSelector.class);
    ColumnCapabilities capabilities = Mockito.mock(ColumnCapabilities.class);
    MultiValueDimensionVectorSelector dimensionSelector = Mockito.mock(MultiValueDimensionVectorSelector.class);
    Mockito.when(columnSelectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of("fieldName")))
           .thenReturn(dimensionSelector);

    Mockito.when(columnSelectorFactory.getColumnCapabilities("fieldName")).thenReturn(capabilities);
    Mockito.when(capabilities.hasMultipleValues()).thenReturn(ColumnCapabilities.Capable.TRUE);

    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);

    VectorAggregator multiValueAggregator = factory.factorizeVector(columnSelectorFactory, selector);

    Assert.assertNotNull(multiValueAggregator);
    Assert.assertEquals(multiValueAggregator.getClass(), StringMaxVectorAggregator.class);
  }

  @Test
  public void testGetComparator()
  {
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    Comparator<String> comparator = factory.getComparator();

    String smaller = "a";
    String larger = "b";

    Assert.assertNotNull(comparator);
    // Max operates in reverse order, so strings that are "smaller" will be "larger" now.
    Assert.assertTrue(comparator.compare(smaller, larger) > 0);
    Assert.assertTrue(comparator.compare(larger, smaller) < 0);
    Assert.assertEquals(0, comparator.compare(larger, "b"));
  }

  @Test
  public void testCombine()
  {
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);

    Assert.assertEquals("b", factory.combine("a", "b"));
    Assert.assertEquals("a", factory.combine("a", "a"));
    Assert.assertEquals("b", factory.combine("b", "a"));
    Assert.assertEquals("a", factory.combine(null, "a"));
    Assert.assertEquals("a", factory.combine("a", null));
    Assert.assertNull(factory.combine(null, null));
  }

  @Test
  public void testGetCombiningFactory()
  {
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    AggregatorFactory combiningFactory = factory.getCombiningFactory();

    Assert.assertNotNull(combiningFactory);
    Assert.assertTrue(combiningFactory instanceof StringMaxAggregatorFactory);
  }

  @Test
  public void testGetMergingFactory() throws AggregatorFactoryNotMergeableException
  {
    StringMaxAggregatorFactory factory1 = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    StringMaxAggregatorFactory factory2 = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);

    AggregatorFactory mergingFactory = factory1.getMergingFactory(factory2);

    Assert.assertNotNull(mergingFactory);
    Assert.assertTrue(mergingFactory instanceof StringMaxAggregatorFactory);
  }

  @Test
  public void testWithName()
  {
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    AggregatorFactory newFactory = factory.withName("newName");

    Assert.assertNotNull(newFactory);
    Assert.assertTrue(newFactory instanceof StringMaxAggregatorFactory);
    Assert.assertEquals("newName", newFactory.getName());
  }

  @Test
  public void testGetCacheKey()
  {
    StringMaxAggregatorFactory factory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    byte[] cacheKey = factory.getCacheKey();

    Assert.assertNotNull(cacheKey);
    Assert.assertTrue(cacheKey.length > 0);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();

    // Initial JSON string
    String json0 = "{\"type\": \"stringMax\", \"name\": \"aggName\", \"fieldName\": \"fieldName\"}";
    StringMaxAggregatorFactory agg0 = objectMapper.readValue(json0, StringMaxAggregatorFactory.class);
    Assert.assertEquals("aggName", agg0.getName());
    Assert.assertEquals("fieldName", agg0.getFieldName());
    Assert.assertEquals((Integer) 1024, agg0.getMaxStringBytes());  // Assuming default value
    Assert.assertTrue(agg0.isAggregateMultipleValues()); // Assuming default value

    // JSON string with all fields
    String aggSpecJson = "{\"type\": \"stringMax\", \"name\": \"aggName\", \"fieldName\": \"fieldName\", \"maxStringBytes\": 2048, \"aggregateMultipleValues\": true}";
    StringMaxAggregatorFactory agg = objectMapper.readValue(aggSpecJson, StringMaxAggregatorFactory.class);
    StringMaxAggregatorFactory expectedAgg = new StringMaxAggregatorFactory("aggName", "fieldName", 2048, true);

    Assert.assertEquals(expectedAgg, agg);

    // Serialize and deserialize to check consistency
    Assert.assertEquals(
        agg,
        objectMapper.readValue(objectMapper.writeValueAsBytes(agg), StringMaxAggregatorFactory.class)
    );
  }
}
