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

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FloatAnyAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";

  @Mock
  private ColumnInspector columnInspector;
  @Mock
  private VectorColumnSelectorFactory selectorFactory;
  @Mock
  private VectorValueSelector valueSelector;

  private FloatAnyAggregatorFactory target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(valueSelector).when(selectorFactory).makeValueSelector(FIELD_NAME);
    target = new FloatAnyAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void canVectorizeShouldReturnTrue()
  {
    Assert.assertTrue(target.canVectorize(columnInspector));
  }

  @Test
  public void factorizeVectorShouldReturnDoubleVectorAggregator()
  {
    VectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assert.assertNotNull(aggregator);
    Assert.assertEquals(FloatAnyVectorAggregator.class, aggregator.getClass());
  }
}
