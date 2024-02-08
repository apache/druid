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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.lookup.LookupJoinMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class LookupJoinMatcherTest
{
  private final Map<String, String> lookupMap =
      ImmutableMap.of("foo", "bar", "null", "", "empty String", "", "", "empty_string");
  private static final String PREFIX = "j.";

  @Mock
  private LookupExtractor extractor;

  @Mock
  private ColumnSelectorFactory leftSelectorFactory;

  @Mock
  private DimensionSelector dimensionSelector;

  private LookupJoinMatcher target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(true).when(extractor).supportsAsMap();
    Mockito.doReturn(lookupMap).when(extractor).asMap();
  }

  @Test
  public void testCreateConditionAlwaysFalseShouldReturnSuccessfullyAndNotThrowException()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("0", PREFIX, ExprMacroTable.nil());
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, false);
    Assert.assertNotNull(target);
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    Assert.assertNotNull(target);
  }

  @Test
  public void testCreateConditionAlwaysTrueShouldReturnSuccessfullyAndNotThrowException()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("1", PREFIX, ExprMacroTable.nil());
    Mockito.doReturn(true).when(extractor).supportsAsMap();
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, false);
    Assert.assertNotNull(target);
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    Assert.assertNotNull(target);
  }

  @Test
  public void testMatchConditionAlwaysTrue()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("1", PREFIX, ExprMacroTable.nil());
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match first
    target.matchCondition();
    Assert.assertTrue(target.hasMatch());
    verifyMatch("foo", "bar");
    // Test match second
    target.nextMatch();
    Assert.assertTrue(target.hasMatch());
    verifyMatch("null", "");
    // Test match third
    target.nextMatch();
    Assert.assertTrue(target.hasMatch());
    verifyMatch("empty String", "");
    // Test match forth
    target.nextMatch();
    Assert.assertTrue(target.hasMatch());
    verifyMatch("", "empty_string");
    // Test no more
    target.nextMatch();
    Assert.assertFalse(target.hasMatch());
  }

  @Test
  public void testMatchConditionAlwaysFalse()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("0", PREFIX, ExprMacroTable.nil());
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match first
    target.matchCondition();
    Assert.assertFalse(target.hasMatch());
    verifyMatch(null, null);
  }

  @Test
  public void testMatchConditionSometimesTrueSometimesFalse()
  {
    final int index = 1;
    SingleIndexedInt row = new SingleIndexedInt();
    row.setValue(index);
    Mockito.doReturn(dimensionSelector).when(leftSelectorFactory).makeDimensionSelector(ArgumentMatchers.any(DimensionSpec.class));
    Mockito.doReturn(row).when(dimensionSelector).getRow();
    Mockito.doReturn("foo").when(dimensionSelector).lookupName(index);
    Mockito.doReturn("bar").when(extractor).apply("foo");

    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression(
        StringUtils.format("\"%sk\" == foo", PREFIX),
        PREFIX,
        ExprMacroTable.nil()
    );
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match
    target.matchCondition();
    Assert.assertTrue(target.hasMatch());
    verifyMatch("foo", "bar");
    // Test no more
    target.nextMatch();
    Assert.assertFalse(target.hasMatch());
  }

  @Test(expected = QueryUnsupportedException.class)
  public void testMatchMultiValuedRowShouldThrowException()
  {
    ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{2, 4, 6});
    Mockito.doReturn(dimensionSelector).when(leftSelectorFactory).makeDimensionSelector(ArgumentMatchers.any(DimensionSpec.class));
    Mockito.doReturn(row).when(dimensionSelector).getRow();

    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression(
        StringUtils.format("\"%sk\" == foo", PREFIX),
        PREFIX,
        ExprMacroTable.nil()
    );
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match should throw exception
    target.matchCondition();
  }

  @Test
  public void testMatchEmptyRow()
  {
    ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{});
    Mockito.doReturn(dimensionSelector).when(leftSelectorFactory).makeDimensionSelector(ArgumentMatchers.any(DimensionSpec.class));
    Mockito.doReturn(row).when(dimensionSelector).getRow();

    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression(
        StringUtils.format("\"%sk\" == foo", PREFIX),
        PREFIX,
        ExprMacroTable.nil()
    );
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    target.matchCondition();
    Assert.assertFalse(target.hasMatch());
  }

  private void verifyMatch(String expectedKey, String expectedValue)
  {
    DimensionSelector selector = target.getColumnSelectorFactory()
                                             .makeDimensionSelector(DefaultDimensionSpec.of("k"));
    Assert.assertEquals(-1, selector.getValueCardinality());
    Assert.assertEquals(expectedKey, selector.lookupName(0));
    Assert.assertEquals(expectedKey, selector.lookupName(0));
    Assert.assertNull(selector.idLookup());

    selector = target.getColumnSelectorFactory()
                                             .makeDimensionSelector(DefaultDimensionSpec.of("v"));
    Assert.assertEquals(-1, selector.getValueCardinality());
    Assert.assertEquals(expectedValue, selector.lookupName(0));
    Assert.assertEquals(expectedValue, selector.lookupName(0));
    Assert.assertNull(selector.idLookup());
  }
}
