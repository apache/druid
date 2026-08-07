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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
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

  @BeforeEach
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
    Assertions.assertNotNull(target);
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    Assertions.assertNotNull(target);
  }

  @Test
  public void testCreateConditionAlwaysTrueShouldReturnSuccessfullyAndNotThrowException()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("1", PREFIX, ExprMacroTable.nil());
    Mockito.doReturn(true).when(extractor).supportsAsMap();
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, false);
    Assertions.assertNotNull(target);
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    Assertions.assertNotNull(target);
  }

  @Test
  public void testMatchConditionAlwaysTrue()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("1", PREFIX, ExprMacroTable.nil());
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match first
    target.matchCondition();
    Assertions.assertTrue(target.hasMatch());
    verifyMatch("foo", "bar");
    // Test match second
    target.nextMatch();
    Assertions.assertTrue(target.hasMatch());
    verifyMatch("null", "");
    // Test match third
    target.nextMatch();
    Assertions.assertTrue(target.hasMatch());
    verifyMatch("empty String", "");
    // Test match forth
    target.nextMatch();
    Assertions.assertTrue(target.hasMatch());
    verifyMatch("", "empty_string");
    // Test no more
    target.nextMatch();
    Assertions.assertFalse(target.hasMatch());
  }

  @Test
  public void testMatchConditionAlwaysFalse()
  {
    JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression("0", PREFIX, ExprMacroTable.nil());
    target = LookupJoinMatcher.create(extractor, leftSelectorFactory, condition, true);
    // Test match first
    target.matchCondition();
    Assertions.assertFalse(target.hasMatch());
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
    Assertions.assertTrue(target.hasMatch());
    verifyMatch("foo", "bar");
    // Test no more
    target.nextMatch();
    Assertions.assertFalse(target.hasMatch());
  }

  @Test
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
    Assertions.assertThrows(QueryUnsupportedException.class, () -> target.matchCondition());
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
    Assertions.assertFalse(target.hasMatch());
  }

  private void verifyMatch(String expectedKey, String expectedValue)
  {
    DimensionSelector selector = target.getColumnSelectorFactory()
                                             .makeDimensionSelector(DefaultDimensionSpec.of("k"));
    Assertions.assertEquals(-1, selector.getValueCardinality());
    Assertions.assertEquals(expectedKey, selector.lookupName(0));
    Assertions.assertEquals(expectedKey, selector.lookupName(0));
    Assertions.assertNull(selector.idLookup());

    selector = target.getColumnSelectorFactory()
                                             .makeDimensionSelector(DefaultDimensionSpec.of("v"));
    Assertions.assertEquals(-1, selector.getValueCardinality());
    Assertions.assertEquals(expectedValue, selector.lookupName(0));
    Assertions.assertEquals(expectedValue, selector.lookupName(0));
    Assertions.assertNull(selector.idLookup());
  }
}
