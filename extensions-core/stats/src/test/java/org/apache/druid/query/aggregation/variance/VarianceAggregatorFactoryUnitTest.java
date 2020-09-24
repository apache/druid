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

package org.apache.druid.query.aggregation.variance;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
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
public class VarianceAggregatorFactoryUnitTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String DOUBLE = "double";
  private static final String LONG = "long";
  private static final String VARIANCE = "variance";
  private static final String UNKNOWN = "unknown";

  @Mock
  private ColumnCapabilities capabilities;
  @Mock
  private VectorColumnSelectorFactory selectorFactory;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private ColumnSelectorFactory metricFactory;

  private VarianceAggregatorFactory target;

  @Before
  public void setup()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void factorizeVectorShouldReturnFloatVectorAggregator()
  {
    VectorAggregator agg = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceFloatVectorAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeVectorForDoubleShouldReturnFloatVectorAggregator()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME, null, DOUBLE);
    VectorAggregator agg = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceDoubleVectorAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeVectorForLongShouldReturnFloatVectorAggregator()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME, null, LONG);
    VectorAggregator agg = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceLongVectorAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeVectorForVarianceShouldReturnObjectVectorAggregator()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME, null, VARIANCE);
    VectorAggregator agg = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceObjectVectorAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeVectorForComplexShouldReturnObjectVectorAggregator()
  {
    mockType(ValueType.COMPLEX);
    VectorAggregator agg = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceObjectVectorAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeBufferedForComplexShouldReturnObjectVectorAggregator()
  {
    mockType(ValueType.COMPLEX);
    BufferAggregator agg = target.factorizeBuffered(metricFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceBufferAggregator.ObjectVarianceAggregator.class, agg.getClass());
  }

  @Test
  public void factorizeForComplexShouldReturnObjectVectorAggregator()
  {
    mockType(ValueType.COMPLEX);
    Aggregator agg = target.factorize(metricFactory);
    Assert.assertNotNull(agg);
    Assert.assertEquals(VarianceAggregator.ObjectVarianceAggregator.class, agg.getClass());
  }

  @Test(expected = IAE.class)
  public void factorizeVectorForUnknownColumnShouldThrowIAE()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME, null, UNKNOWN);
    target.factorizeVector(selectorFactory);
  }

  @Test(expected = IAE.class)
  public void factorizeBufferedForUnknownColumnShouldThrowIAE()
  {
    target = new VarianceAggregatorFactory(NAME, FIELD_NAME, null, UNKNOWN);
    target.factorizeBuffered(metricFactory);
  }

  @Test
  public void equalsContract()
  {
    EqualsVerifier.forClass(VarianceAggregatorFactory.class)
                  .usingGetClass()
                  .verify();
  }

  private void mockType(ValueType type)
  {
    Mockito.doReturn(capabilities).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(capabilities).when(metricFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(type).when(capabilities).getType();
  }
}
