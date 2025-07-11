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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Bitmap64ExactCountBuildAggregatorFactoryTest
{
  private static final String NAME = "exactCountBuildTestName";
  private static final String FIELD_NAME = "exactCountBuildTestFieldName";

  private Bitmap64ExactCountBuildAggregatorFactory factory;

  @BeforeEach
  public void setUp()
  {
    factory = new Bitmap64ExactCountBuildAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void testConstructor()
  {
    Assertions.assertEquals(NAME, factory.getName());
    Assertions.assertEquals(FIELD_NAME, factory.getFieldName());
  }

  @Test
  public void testGetCacheTypeId()
  {
    Assertions.assertEquals(AggregatorUtil.BITMAP64_EXACT_COUNT_BUILD_CACHE_TYPE_ID, factory.getCacheTypeId());
  }

  @Test
  public void testFactorize()
  {
    ColumnSelectorFactory selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.getColumnCapabilities(FIELD_NAME)).andReturn(null);
    EasyMock.expect(selectorFactory.makeColumnValueSelector(FIELD_NAME))
            .andReturn(new TestObjectColumnSelector<>(null)); // Return a dummy selector
    EasyMock.replay(selectorFactory);

    Assertions.assertInstanceOf(Bitmap64ExactCountBuildAggregator.class, factory.factorize(selectorFactory));
    EasyMock.verify(selectorFactory);
  }

  @Test
  public void testFactorizeWithNonNumericColumnThrowsIAE()
  {
    ColumnSelectorFactory selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    ColumnCapabilities capabilities = EasyMock.createMock(ColumnCapabilities.class);

    EasyMock.expect(selectorFactory.getColumnCapabilities(FIELD_NAME)).andReturn(capabilities);
    EasyMock.expect(capabilities.getType()).andReturn(ValueType.STRING);

    EasyMock.replay(selectorFactory, capabilities);

    Assertions.assertThrows(
        IAE.class,
        () -> factory.factorize(selectorFactory)
    );

    EasyMock.verify(selectorFactory, capabilities);
  }

  @Test
  public void testFactorizeBuffered()
  {
    ColumnSelectorFactory selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.getColumnCapabilities(FIELD_NAME)).andReturn(null);
    EasyMock.expect(selectorFactory.makeColumnValueSelector(FIELD_NAME))
            .andReturn(new TestObjectColumnSelector<>(null)); // Return a dummy selector
    EasyMock.replay(selectorFactory);

    Assertions.assertInstanceOf(
        Bitmap64ExactCountBuildBufferAggregator.class,
        factory.factorizeBuffered(selectorFactory)
    );
    EasyMock.verify(selectorFactory);
  }

  @Test
  public void testGetIntermediateType()
  {
    Assertions.assertEquals(Bitmap64ExactCountBuildAggregatorFactory.TYPE, factory.getIntermediateType());
  }

  @Test
  public void testGetResultType()
  {
    Assertions.assertEquals(ColumnType.LONG, factory.getResultType());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    Bitmap64ExactCountBuildAggregatorFactory factory1 = new Bitmap64ExactCountBuildAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Bitmap64ExactCountBuildAggregatorFactory factory2 = new Bitmap64ExactCountBuildAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Bitmap64ExactCountBuildAggregatorFactory factoryDiffName = new Bitmap64ExactCountBuildAggregatorFactory(
        NAME + "_diff",
        FIELD_NAME
    );
    Bitmap64ExactCountBuildAggregatorFactory factoryDiffFieldName = new Bitmap64ExactCountBuildAggregatorFactory(
        NAME,
        FIELD_NAME + "_diff"
    );

    Assertions.assertEquals(factory1, factory2);
    Assertions.assertEquals(factory1.hashCode(), factory2.hashCode());

    Assertions.assertNotEquals(factory1, factoryDiffName);
    Assertions.assertNotEquals(factory1.hashCode(), factoryDiffName.hashCode());

    Assertions.assertNotEquals(factory1, factoryDiffFieldName);
    Assertions.assertNotEquals(factory1.hashCode(), factoryDiffFieldName.hashCode());
  }

  @Test
  public void testToString()
  {
    String expected = "Bitmap64ExactCountBuildAggregatorFactory { name=" + NAME + ", fieldName=" + FIELD_NAME + " }";
    Assertions.assertEquals(expected, factory.toString());
  }
}
