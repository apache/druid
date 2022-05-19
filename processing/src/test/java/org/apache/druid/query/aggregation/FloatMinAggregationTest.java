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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class FloatMinAggregationTest
{
  private final FloatMinAggregatorFactory floatMinAggregatorFactory;
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;

  private final float[] floatValues1 = {5f, 2f, 4f, 100f, 1f, 5f, -2f, -3f, 0f, 55f};

  public FloatMinAggregationTest() throws Exception
  {
    String vectorAggSpecJson = "{\"type\": \"floatMin\", \"name\": \"flt\", \"fieldName\": \"fltFld\"}";
    floatMinAggregatorFactory = TestHelper.makeJsonMapper().readValue(vectorAggSpecJson, FloatMinAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();

    VectorValueSelector vectorValueSelector = EasyMock.createMock(VectorValueSelector.class);
    EasyMock.expect(vectorValueSelector.getFloatVector()).andReturn(floatValues1).anyTimes();
    EasyMock.expect(vectorValueSelector.getNullVector()).andReturn(null).anyTimes();
    EasyMock.replay(vectorValueSelector);

    vectorColumnSelectorFactory = EasyMock.createMock(VectorColumnSelectorFactory.class);
    EasyMock.expect(vectorColumnSelectorFactory.getColumnCapabilities("fltFld"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.FLOAT).setDictionaryEncoded(true)).anyTimes();
    EasyMock.expect(vectorColumnSelectorFactory.makeValueSelector("fltFld")).andReturn(vectorValueSelector).anyTimes();
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void testFloatMinVectorAggregator()
  {
    // Some sanity.
    Assert.assertTrue(floatMinAggregatorFactory.canVectorize(vectorColumnSelectorFactory));
    VectorValueSelector vectorValueSelector = floatMinAggregatorFactory.vectorSelector(vectorColumnSelectorFactory);
    Assert.assertEquals(floatValues1, vectorValueSelector.getFloatVector());

    VectorAggregator vectorAggregator = floatMinAggregatorFactory.factorizeVector(vectorColumnSelectorFactory);

    final ByteBuffer buf = ByteBuffer.allocate(floatMinAggregatorFactory.getMaxIntermediateSizeWithNulls() * 3);
    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 3);
    Assert.assertEquals(floatValues1[1], vectorAggregator.get(buf, 0));

    vectorAggregator.init(buf, 4);
    vectorAggregator.aggregate(buf, 4, 0, 3);
    Assert.assertEquals(floatValues1[1], vectorAggregator.get(buf, 4));

    vectorAggregator.init(buf, 8);
    vectorAggregator.aggregate(buf, 8, 3, 7);
    Assert.assertEquals(floatValues1[6], vectorAggregator.get(buf, 8));

    vectorAggregator.init(buf, 0);
    vectorAggregator.aggregate(buf, 0, 0, 10);
    Assert.assertEquals(floatValues1[7], vectorAggregator.get(buf, 0));
  }
}
