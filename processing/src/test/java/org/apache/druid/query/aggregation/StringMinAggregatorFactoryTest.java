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
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Comparator;

public class StringMinAggregatorFactoryTest
{
  private final ExprMacroTable macroTable = ExprMacroTable.nil();

  @Test
  public void testBuildAggregators()
  {
    @SuppressWarnings("unchecked")
    BaseObjectColumnValueSelector<String> selector = Mockito.mock(BaseObjectColumnValueSelector.class);
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);

    Assert.assertEquals(factory, new StringMinAggregatorFactory("name", "fieldName", null, true, null, macroTable));
    Assert.assertEquals(
        factory.toString(),
        "StringMinAggregatorFactory{name='name', fieldName='fieldName', maxStringBytes=1024, aggregateMultipleValues=true}"
    );

    Aggregator aggregator = factory.buildAggregator(selector);
    Assert.assertNotNull(aggregator);
    Assert.assertEquals(StringMinAggregator.class, aggregator.getClass());

    BufferAggregator bufferAggregator = factory.buildBufferAggregator(selector);
    Assert.assertNotNull(bufferAggregator);
    Assert.assertEquals(StringMinBufferAggregator.class, bufferAggregator.getClass());
  }

  @Test
  public void testFailingBuilds()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new StringMinAggregatorFactory("name", "fieldName", -1024, true)
    );
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new StringMinAggregatorFactory("name", "fieldName", Integer.MAX_VALUE - 3, true)
    );
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new StringMinAggregatorFactory("name", "fieldName", null, true, "fieldName", macroTable)
    );
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new StringMinAggregatorFactory("name", null, null, true, null, macroTable)
    );
  }

  @Test
  public void testFactorizeVectorOnMultiValueDimensionSelectorWithNullExpression()
  {
    VectorColumnSelectorFactory columnSelectorFactory = Mockito.mock(VectorColumnSelectorFactory.class);
    VectorValueSelector selector = Mockito.mock(VectorValueSelector.class);
    ColumnCapabilities capabilities = Mockito.mock(ColumnCapabilities.class);
    MultiValueDimensionVectorSelector dimensionSelector = Mockito.mock(MultiValueDimensionVectorSelector.class);
    Mockito.when(columnSelectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of("fieldName")))
           .thenReturn(dimensionSelector);

    Mockito.when(columnSelectorFactory.getColumnCapabilities("fieldName")).thenReturn(capabilities);
    Mockito.when(capabilities.hasMultipleValues()).thenReturn(ColumnCapabilities.Capable.TRUE);

    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);

    VectorAggregator multiValueAggregator = factory.factorizeVector(columnSelectorFactory, selector);

    Assert.assertNotNull(multiValueAggregator);
    Assert.assertEquals(multiValueAggregator.getClass(), StringMinVectorAggregator.class);
  }

  @Test
  public void testFactorizeVectorOnSingleValueDimensionVectorSelectorWithNullFieldName()
  {
    VectorColumnSelectorFactory falseCapabilitiesColumnSelectorFactory = Mockito.mock(VectorColumnSelectorFactory.class);
    VectorValueSelector falseCapabilitiesSelector = Mockito.mock(VectorValueSelector.class);
    ColumnCapabilities falseCapabilities = Mockito.mock(ColumnCapabilities.class);
    SingleValueDimensionVectorSelector falseCapabilitiesDimensionSelector = Mockito.mock(
        SingleValueDimensionVectorSelector.class);
    Mockito.when(falseCapabilitiesColumnSelectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(
               "fieldName")))
           .thenReturn(falseCapabilitiesDimensionSelector);

    Mockito.when(falseCapabilitiesColumnSelectorFactory.getColumnCapabilities("fieldName"))
           .thenReturn(falseCapabilities);
    Mockito.when(falseCapabilities.hasMultipleValues()).thenReturn(ColumnCapabilities.Capable.FALSE);

    StringMinAggregatorFactory falseCapabilitiesFactory = new StringMinAggregatorFactory(
        "name",
        null,
        1024,
        true,
        "fieldName",
        macroTable
    );
    VectorAggregator falseCapabilitiesSingleValueAggregator = falseCapabilitiesFactory.factorizeVector(
        falseCapabilitiesColumnSelectorFactory,
        falseCapabilitiesSelector
    );
    Assert.assertNotNull(falseCapabilitiesSingleValueAggregator);
    Assert.assertEquals(falseCapabilitiesSingleValueAggregator.getClass(), StringMinVectorAggregator.class);

    VectorColumnSelectorFactory nullCapabilitiesColumnSelectorFactory = Mockito.mock(VectorColumnSelectorFactory.class);
    VectorValueSelector nullCapabilitiesDimensionSelector = Mockito.mock(VectorValueSelector.class);
    SingleValueDimensionVectorSelector nullValueSelector = Mockito.mock(SingleValueDimensionVectorSelector.class);
    Mockito.when(nullCapabilitiesColumnSelectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(
               "fieldName")))
           .thenReturn(nullValueSelector);
    Mockito.when(nullCapabilitiesColumnSelectorFactory.getColumnCapabilities("fieldName")).thenReturn(null);

    StringMinAggregatorFactory nullCapabilitiesFactory = new StringMinAggregatorFactory(
        "name",
        null,
        1024,
        true,
        "fieldName",
        macroTable
    );

    VectorAggregator nullCapabilitiesSingleValueAggregator = nullCapabilitiesFactory.factorizeVector(
        nullCapabilitiesColumnSelectorFactory,
        nullCapabilitiesDimensionSelector
    );

    Assert.assertNotNull(nullCapabilitiesSingleValueAggregator);
    Assert.assertEquals(nullCapabilitiesSingleValueAggregator.getClass(), StringMinVectorAggregator.class);
  }

  @Test
  public void testGetComparator()
  {
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);
    Comparator<String> comparator = factory.getComparator();

    String smaller = "a";
    String larger = "b";

    Assert.assertNotNull(comparator);
    Assert.assertTrue(comparator.compare(smaller, larger) < 0);
    Assert.assertTrue(comparator.compare(larger, smaller) > 0);
    Assert.assertEquals(0, comparator.compare(larger, "b"));
  }

  @Test
  public void testCombine()
  {
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);

    Assert.assertEquals("a", factory.combine("a", "b"));
    Assert.assertEquals("a", factory.combine("a", "a"));
    Assert.assertEquals("a", factory.combine("b", "a"));
    Assert.assertEquals("a", factory.combine(null, "a"));
    Assert.assertEquals("a", factory.combine("a", null));
    Assert.assertNull(factory.combine(null, null));
  }

  @Test
  public void testGetCombiningFactory()
  {
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);
    AggregatorFactory combiningFactory = factory.getCombiningFactory();

    Assert.assertNotNull(combiningFactory);
    Assert.assertTrue(combiningFactory instanceof StringMinAggregatorFactory);
  }

  @Test
  public void testGetMergingFactory() throws AggregatorFactoryNotMergeableException
  {
    StringMinAggregatorFactory factory1 = new StringMinAggregatorFactory("name", "fieldName", 1024, true);
    StringMinAggregatorFactory factory2 = new StringMinAggregatorFactory("name", "fieldName", 1024, true);

    AggregatorFactory mergingFactory = factory1.getMergingFactory(factory2);

    Assert.assertNotNull(mergingFactory);
    Assert.assertTrue(mergingFactory instanceof StringMinAggregatorFactory);

    StringMinAggregatorFactory mismatchedFactory = new StringMinAggregatorFactory(
        "notSameName",
        "fieldName",
        1024,
        true
    );
    Assert.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> mismatchedFactory.getMergingFactory(factory1)
    );

    StringMaxAggregatorFactory mismatchClassFactory = new StringMaxAggregatorFactory("name", "fieldName", 1024, true);
    Assert.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> mismatchClassFactory.getMergingFactory(factory1)
    );
  }

  @Test
  public void testWithName()
  {
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);
    AggregatorFactory newFactory = factory.withName("newName");

    Assert.assertNotNull(newFactory);
    Assert.assertTrue(newFactory instanceof StringMinAggregatorFactory);
    Assert.assertEquals("newName", newFactory.getName());
  }

  @Test
  public void testGetCacheKey()
  {
    StringMinAggregatorFactory factory = new StringMinAggregatorFactory("name", "fieldName", 1024, true);
    byte[] cacheKey = factory.getCacheKey();

    Assert.assertNotNull(cacheKey);
    Assert.assertTrue(cacheKey.length > 0);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();

    // Initial JSON string
    String json0 = "{\"type\": \"stringMin\", \"name\": \"aggName\", \"fieldName\": \"fieldName\"}";
    StringMinAggregatorFactory agg0 = objectMapper.readValue(json0, StringMinAggregatorFactory.class);
    Assert.assertEquals("aggName", agg0.getName());
    Assert.assertEquals("fieldName", agg0.getFieldName());
    Assert.assertEquals((Integer) 1024, agg0.getMaxStringBytes());  // Assuming default value
    Assert.assertTrue(agg0.isAggregateMultipleValues()); // Assuming default value

    // JSON string with all fields
    String aggSpecJson = "{\"type\": \"stringMin\", \"name\": \"aggName\", \"fieldName\": \"fieldName\", \"maxStringBytes\": 2048, \"aggregateMultipleValues\": true}";
    StringMinAggregatorFactory agg = objectMapper.readValue(aggSpecJson, StringMinAggregatorFactory.class);
    StringMinAggregatorFactory expectedAgg = new StringMinAggregatorFactory("aggName", "fieldName", 2048, true);

    Assert.assertEquals(expectedAgg, agg);

    // Serialize and deserialize to check consistency
    Assert.assertEquals(
        agg,
        objectMapper.readValue(objectMapper.writeValueAsBytes(agg), StringMinAggregatorFactory.class)
    );
  }
}
