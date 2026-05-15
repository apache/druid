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
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;

@ExtendWith(MockitoExtension.class)
public class LongAnyAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final int POSITION = 2;
  private static final ByteBuffer BUFFER = ByteBuffer.allocate(128);

  @Mock
  private ColumnCapabilities capabilities;
  @Mock
  private ColumnInspector columnInspector;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private VectorColumnSelectorFactory selectorFactory;
  @Mock
  private VectorValueSelector valueSelector;

  private LongAnyAggregatorFactory target;

  @BeforeEach
  public void setUp()
  {
    target = new LongAnyAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void canVectorizeShouldReturnTrue()
  {
    Assertions.assertTrue(target.canVectorize(columnInspector));
  }

  @Test
  public void factorizeVectorShouldReturnLongVectorAggregator()
  {
    Mockito.doReturn(null).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(valueSelector).when(selectorFactory).makeValueSelector(FIELD_NAME);

    VectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assertions.assertNotNull(aggregator);
    Assertions.assertEquals(LongAnyVectorAggregator.class, aggregator.getClass());
  }

  @Test
  public void factorizeVectorWithNumericColumnShouldReturnLongVectorAggregator()
  {
    Mockito.doReturn(capabilities).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(true).when(capabilities).isNumeric();
    VectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assertions.assertNotNull(aggregator);
    Assertions.assertEquals(LongAnyVectorAggregator.class, aggregator.getClass());
  }

  @Test
  public void factorizeVectorForStringTypeShouldReturnLongVectorAggregatorWithNilSelector()
  {
    Mockito.doReturn(capabilities).when(selectorFactory).getColumnCapabilities(FIELD_NAME);
    Mockito.doReturn(false).when(capabilities).isNumeric();
    VectorAggregator aggregator = target.factorizeVector(selectorFactory);
    Assertions.assertNotNull(aggregator);
    Assertions.assertNull(aggregator.get(BUFFER, POSITION));
  }
}
