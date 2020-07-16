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

package org.apache.druid.query.filter.vector;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VectorValueMatcherColumnProcessorFactoryTest extends InitializedNullHandlingTest
{
  private static final int VECTOR_SIZE = 128;
  private static final int CURRENT_SIZE = 24;
  private VectorValueSelector vectorValueSelector;

  @Before
  public void setup()
  {
    vectorValueSelector = EasyMock.createMock(VectorValueSelector.class);
    EasyMock.expect(vectorValueSelector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(vectorValueSelector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.replay(vectorValueSelector);
  }

  @Test
  public void testFloat()
  {
    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeFloatProcessor(vectorValueSelector);

    Assert.assertTrue(matcherFactory instanceof FloatVectorValueMatcher);

    VectorValueMatcher matcher = matcherFactory.makeMatcher("2.0");
    Assert.assertFalse(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());

    // in default mode, matching null produces a boolean matcher
    VectorValueMatcher booleanMatcher = matcherFactory.makeMatcher((String) null);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertTrue(booleanMatcher instanceof BooleanVectorValueMatcher);
    } else {
      Assert.assertFalse(booleanMatcher instanceof BooleanVectorValueMatcher);
    }
    Assert.assertEquals(VECTOR_SIZE, booleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, booleanMatcher.getCurrentVectorSize());
    EasyMock.verify(vectorValueSelector);
  }

  @Test
  public void testDouble()
  {
    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeDoubleProcessor(vectorValueSelector);

    Assert.assertTrue(matcherFactory instanceof DoubleVectorValueMatcher);


    VectorValueMatcher matcher = matcherFactory.makeMatcher("1.0");
    Assert.assertFalse(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());

    // in default mode, matching null produces a boolean matcher
    VectorValueMatcher booleanMatcher = matcherFactory.makeMatcher((String) null);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertTrue(booleanMatcher instanceof BooleanVectorValueMatcher);
    } else {
      Assert.assertFalse(booleanMatcher instanceof BooleanVectorValueMatcher);
    }
    Assert.assertEquals(VECTOR_SIZE, booleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, booleanMatcher.getCurrentVectorSize());
    EasyMock.verify(vectorValueSelector);
  }

  @Test
  public void testLong()
  {
    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeLongProcessor(vectorValueSelector);

    Assert.assertTrue(matcherFactory instanceof LongVectorValueMatcher);

    VectorValueMatcher matcher = matcherFactory.makeMatcher("1");
    Assert.assertFalse(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());

    // in default mode, matching null produces a boolean matcher
    VectorValueMatcher booleanMatcher = matcherFactory.makeMatcher((String) null);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertTrue(booleanMatcher instanceof BooleanVectorValueMatcher);
    } else {
      Assert.assertFalse(booleanMatcher instanceof BooleanVectorValueMatcher);
    }
    Assert.assertEquals(VECTOR_SIZE, booleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, booleanMatcher.getCurrentVectorSize());
    EasyMock.verify(vectorValueSelector);
  }

  @Test
  public void testSingleValueString()
  {
    IdLookup lookup = EasyMock.createMock(IdLookup.class);
    SingleValueDimensionVectorSelector selector =
        EasyMock.createMock(SingleValueDimensionVectorSelector.class);
    EasyMock.expect(selector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(selector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.expect(selector.getValueCardinality()).andReturn(1024).anyTimes();
    EasyMock.expect(selector.nameLookupPossibleInAdvance()).andReturn(false).anyTimes();
    EasyMock.expect(selector.idLookup()).andReturn(lookup).anyTimes();
    EasyMock.expect(lookup.lookupId("any value")).andReturn(1).anyTimes();
    EasyMock.expect(lookup.lookupId("another value")).andReturn(-1).anyTimes();
    EasyMock.replay(selector, lookup);

    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeSingleValueDimensionProcessor(selector);

    Assert.assertTrue(matcherFactory instanceof SingleValueStringVectorValueMatcher);

    // value exists in column nonboolean matcher
    VectorValueMatcher matcher = matcherFactory.makeMatcher("any value");
    Assert.assertFalse(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());

    // value not exist in dictionary uses boolean matcher
    VectorValueMatcher booleanMatcher = matcherFactory.makeMatcher("another value");
    Assert.assertTrue(booleanMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, booleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, booleanMatcher.getCurrentVectorSize());
    EasyMock.verify(selector, lookup);
  }

  @Test
  public void testSingleValueStringZeroCardinalityAlwaysBooleanMatcher()
  {
    // cardinality 0 has special path to always use boolean matcher
    SingleValueDimensionVectorSelector selector =
        EasyMock.createMock(SingleValueDimensionVectorSelector.class);
    EasyMock.expect(selector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(selector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.expect(selector.getValueCardinality()).andReturn(0).anyTimes();
    EasyMock.replay(selector);

    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeSingleValueDimensionProcessor(selector);

    Assert.assertTrue(matcherFactory instanceof SingleValueStringVectorValueMatcher);

    VectorValueMatcher matcher = matcherFactory.makeMatcher("any value");
    Assert.assertTrue(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());

    // all are boolean with no valued column i guess
    VectorValueMatcher anotherMatcher = matcherFactory.makeMatcher((String) null);
    Assert.assertTrue(anotherMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, anotherMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, anotherMatcher.getCurrentVectorSize());
    EasyMock.verify(selector);
  }

  @Test
  public void testSingleValueStringOneCardinalityBooleanMatcherIfNullAndNameLookupPossible()
  {
    // single value string column with cardinality 1 and name lookup possible in advance uses boolean matcher for
    // matches
    SingleValueDimensionVectorSelector selector =
        EasyMock.createMock(SingleValueDimensionVectorSelector.class);
    EasyMock.expect(selector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(selector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.expect(selector.getValueCardinality()).andReturn(1).anyTimes();
    EasyMock.expect(selector.nameLookupPossibleInAdvance()).andReturn(true).anyTimes();
    EasyMock.expect(selector.lookupName(0)).andReturn(null).anyTimes();
    EasyMock.replay(selector);

    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeSingleValueDimensionProcessor(selector);

    Assert.assertTrue(matcherFactory instanceof SingleValueStringVectorValueMatcher);

    // false matcher
    VectorValueMatcher booleanMatcher = matcherFactory.makeMatcher("any value");
    Assert.assertTrue(booleanMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, booleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, booleanMatcher.getCurrentVectorSize());

    // true matcher
    VectorValueMatcher anotherBooleanMatcher = matcherFactory.makeMatcher((String) null);
    Assert.assertTrue(anotherBooleanMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, anotherBooleanMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, anotherBooleanMatcher.getCurrentVectorSize());
    EasyMock.verify(selector);
  }

  @Test
  public void testSingleValueStringOneCardinalityBooleanMatcherIfNullAndNameLookupNotPossible()
  {
    // if name lookup not possible in advance, use normal path, even if cardinality 1
    IdLookup lookup = EasyMock.createMock(IdLookup.class);
    SingleValueDimensionVectorSelector selector =
        EasyMock.createMock(SingleValueDimensionVectorSelector.class);
    EasyMock.expect(selector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(selector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.expect(selector.getValueCardinality()).andReturn(1).anyTimes();
    EasyMock.expect(selector.nameLookupPossibleInAdvance()).andReturn(false).anyTimes();
    EasyMock.expect(selector.idLookup()).andReturn(lookup).anyTimes();
    EasyMock.expect(lookup.lookupId("any value")).andReturn(1).anyTimes();
    EasyMock.expect(lookup.lookupId(null)).andReturn(0).anyTimes();
    EasyMock.replay(selector, lookup);

    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeSingleValueDimensionProcessor(selector);

    Assert.assertTrue(matcherFactory instanceof SingleValueStringVectorValueMatcher);

    VectorValueMatcher matcher = matcherFactory.makeMatcher("any value");
    Assert.assertFalse(matcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, matcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, matcher.getCurrentVectorSize());
    EasyMock.verify(selector, lookup);
  }

  @Test
  public void testMultiValueString()
  {
    IdLookup lookup = EasyMock.createMock(IdLookup.class);
    MultiValueDimensionVectorSelector selector = EasyMock.createMock(MultiValueDimensionVectorSelector.class);
    EasyMock.expect(selector.getCurrentVectorSize()).andReturn(CURRENT_SIZE).anyTimes();
    EasyMock.expect(selector.getMaxVectorSize()).andReturn(VECTOR_SIZE).anyTimes();
    EasyMock.expect(selector.getValueCardinality()).andReturn(11).anyTimes();
    EasyMock.expect(selector.nameLookupPossibleInAdvance()).andReturn(false).anyTimes();
    EasyMock.expect(selector.idLookup()).andReturn(lookup).anyTimes();
    EasyMock.expect(lookup.lookupId("any value")).andReturn(-1).anyTimes();
    EasyMock.expect(lookup.lookupId(null)).andReturn(0).anyTimes();
    EasyMock.replay(selector, lookup);
    VectorValueMatcherFactory matcherFactory =
        VectorValueMatcherColumnProcessorFactory.instance().makeMultiValueDimensionProcessor(selector);

    Assert.assertTrue(matcherFactory instanceof MultiValueStringVectorValueMatcher);

    VectorValueMatcher valueNotExistMatcher = matcherFactory.makeMatcher("any value");
    Assert.assertTrue(valueNotExistMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, valueNotExistMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, valueNotExistMatcher.getCurrentVectorSize());

    VectorValueMatcher valueExistMatcher = matcherFactory.makeMatcher((String) null);
    Assert.assertFalse(valueExistMatcher instanceof BooleanVectorValueMatcher);
    Assert.assertEquals(VECTOR_SIZE, valueExistMatcher.getMaxVectorSize());
    Assert.assertEquals(CURRENT_SIZE, valueExistMatcher.getCurrentVectorSize());
    EasyMock.verify(selector, lookup);
  }
}
