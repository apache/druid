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
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class StringAnyAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final int MAX_STRING_BYTES = 10;

  @Mock
  private ColumnInspector columnInspector;
  @Mock
  private ColumnCapabilities capabilities;
  @Mock
  private VectorColumnSelectorFactory vectorSelectorFactory;
  @Mock
  private SingleValueDimensionVectorSelector singleValueDimensionVectorSelector;
  @Mock
  private MultiValueDimensionVectorSelector multiValueDimensionVectorSelector;

  private StringAnyAggregatorFactory target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(capabilities).when(vectorSelectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(ColumnCapabilities.Capable.UNKNOWN).when(capabilities).hasMultipleValues();
    target = new StringAnyAggregatorFactory(NAME, FIELD_NAME, MAX_STRING_BYTES);
  }

  @Test
  public void canVectorizeWithoutCapabilitiesShouldReturnTrue()
  {
    Assert.assertTrue(target.canVectorize(columnInspector));
  }

  @Test
  public void factorizeVectorWithoutCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    Mockito.doReturn(null).when(vectorSelectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(singleValueDimensionVectorSelector)
           .when(vectorSelectorFactory)
           .makeSingleValueDimensionSelector(any());
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithUnknownCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    Mockito.doReturn(multiValueDimensionVectorSelector)
           .when(vectorSelectorFactory)
           .makeMultiValueDimensionSelector(any());
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithMultipleValuesCapabilitiesShouldReturnAggregatorWithMultiDimensionSelector()
  {
    Mockito.doReturn(ColumnCapabilities.Capable.TRUE).when(capabilities).hasMultipleValues();
    Mockito.doReturn(multiValueDimensionVectorSelector)
           .when(vectorSelectorFactory)
           .makeMultiValueDimensionSelector(any());
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }

  @Test
  public void factorizeVectorWithoutMultipleValuesCapabilitiesShouldReturnAggregatorWithSingleDimensionSelector()
  {
    Mockito.doReturn(ColumnCapabilities.Capable.FALSE).when(capabilities).hasMultipleValues();
    Mockito.doReturn(singleValueDimensionVectorSelector)
           .when(vectorSelectorFactory)
           .makeSingleValueDimensionSelector(any());
    StringAnyVectorAggregator aggregator = target.factorizeVector(vectorSelectorFactory);
    Assert.assertNotNull(aggregator);
  }
}
