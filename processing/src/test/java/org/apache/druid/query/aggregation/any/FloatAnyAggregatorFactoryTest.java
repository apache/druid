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

import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FloatAnyAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";

  @Mock
  private ColumnCapabilities capabilities;
  @Mock
  private ColumnInspector columnInspector;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private VectorColumnSelectorFactory selectorFactory;
  @Mock
  private VectorValueSelector valueSelector;

  private FloatAnyAggregatorFactory target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(null).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(valueSelector).when(selectorFactory).makeValueSelector(FIELD_NAME);
    target = new FloatAnyAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void canVectorizeShouldReturnTrue()
  {
    Assert.assertTrue(target.canVectorize(columnInspector));
  }

  @Test
  public void factorizeVectorShouldReturnFloatVectorAggregator()
  {
    FloatAnyVectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(aggregator);
    Assert.assertNotEquals(NilVectorSelector.class, aggregator.vectorValueSelector.getClass());
  }

  @Test
  public void factorizeVectorForNumericTypeShouldReturnFloatVectorAggregator()
  {
    Mockito.doReturn(capabilities).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(ValueType.FLOAT).when(capabilities).getType();
    FloatAnyVectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(aggregator);
    Assert.assertNotEquals(NilVectorSelector.class, aggregator.vectorValueSelector.getClass());
  }

  @Test
  public void factorizeVectorForStringTypeShouldReturnFloatVectorAggregatorWithNilSelector()
  {
    Mockito.doReturn(capabilities).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(ValueType.STRING).when(capabilities).getType();
    FloatAnyVectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(aggregator);
    Assert.assertEquals(NilVectorSelector.class, aggregator.vectorValueSelector.getClass());
  }
}
