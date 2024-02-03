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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GroupByQueryEngineTest
{
  private static final String DIM = "d0";
  ColumnSelectorFactory factory;

  @Before
  public void setUp()
  {
    factory = EasyMock.createMock(ColumnSelectorFactory.class);
  }

  @Test
  public void testCanPushDownLimitForSegmentStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(true)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesSorted(true)
                                                                  .setDictionaryValuesUnique(true);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assert.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForIncrementalStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(false)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(true);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assert.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForExpressionStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(false)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assert.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForJoinStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assert.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForNumericSelector()
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.LONG)
                                                                      .setHasBitmapIndexes(false)
                                                                      .setHasMultipleValues(false)
                                                                      .setDictionaryEncoded(false)
                                                                      .setDictionaryValuesSorted(false)
                                                                      .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).anyTimes();
    EasyMock.replay(factory);
    Assert.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    capabilities.setType(ColumnType.DOUBLE);
    Assert.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    capabilities.setType(ColumnType.FLOAT);
    Assert.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForComplexSelector()
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(new ColumnType(ValueType.COMPLEX, "foo", null))
                                                                      .setHasBitmapIndexes(false)
                                                                      .setHasMultipleValues(false)
                                                                      .setDictionaryEncoded(false)
                                                                      .setDictionaryValuesSorted(false)
                                                                      .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assert.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }
}
