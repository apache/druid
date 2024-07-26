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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ConstantMultiValueDimensionSelectorTest extends InitializedNullHandlingTest
{
  private final DimensionSelector NULL_SELECTOR = DimensionSelector.multiConstant(null);
  private final DimensionSelector EMPTY_SELECTOR = DimensionSelector.multiConstant(Collections.emptyList());
  private final DimensionSelector SINGLE_SELECTOR = DimensionSelector.multiConstant(ImmutableList.of("billy"));
  private final DimensionSelector CONST_SELECTOR = DimensionSelector.multiConstant(ImmutableList.of("billy", "douglas"));
  private final DimensionSelector NULL_EXTRACTION_SELECTOR = DimensionSelector.multiConstant(
      null,
      new StringFormatExtractionFn("billy")
  );
  private final DimensionSelector CONST_EXTRACTION_SELECTOR = DimensionSelector.multiConstant(
      ImmutableList.of("billy", "douglas", "billy"),
      new SubstringDimExtractionFn(0, 4)
  );

  @Test
  public void testGetRow()
  {
    IndexedInts row = NULL_SELECTOR.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    row = EMPTY_SELECTOR.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    row = SINGLE_SELECTOR.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    row = CONST_SELECTOR.getRow();
    Assert.assertEquals(2, row.size());
    Assert.assertEquals(0, row.get(0));
    Assert.assertEquals(1, row.get(1));

    row = NULL_EXTRACTION_SELECTOR.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    row = CONST_EXTRACTION_SELECTOR.getRow();
    Assert.assertEquals(3, row.size());
    Assert.assertEquals(0, row.get(0));
    Assert.assertEquals(1, row.get(1));
    Assert.assertEquals(2, row.get(2));
  }

  @Test
  public void testLookupName()
  {
    Assert.assertNull(NULL_SELECTOR.lookupName(0));

    Assert.assertNull(EMPTY_SELECTOR.lookupName(0));

    Assert.assertEquals("billy", CONST_SELECTOR.lookupName(0));
    Assert.assertEquals("douglas", CONST_SELECTOR.lookupName(1));

    Assert.assertEquals("billy", NULL_EXTRACTION_SELECTOR.lookupName(0));

    Assert.assertEquals("bill", CONST_EXTRACTION_SELECTOR.lookupName(0));
    Assert.assertEquals("doug", CONST_EXTRACTION_SELECTOR.lookupName(1));
    Assert.assertEquals("bill", CONST_EXTRACTION_SELECTOR.lookupName(2));
  }

  @Test
  public void testGetObject()
  {
    Assert.assertNull(NULL_SELECTOR.lookupName(0));

    Assert.assertNull(EMPTY_SELECTOR.lookupName(0));

    Assert.assertEquals("billy", SINGLE_SELECTOR.getObject());
    Assert.assertEquals(ImmutableList.of("billy", "douglas"), CONST_SELECTOR.getObject());

    Assert.assertEquals("billy", NULL_EXTRACTION_SELECTOR.getObject());

    Assert.assertEquals(ImmutableList.of("bill", "doug", "bill"), CONST_EXTRACTION_SELECTOR.getObject());
  }

  @Test
  public void testCoverage()
  {
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, CONST_SELECTOR.getValueCardinality());
    Assert.assertNull(CONST_SELECTOR.idLookup());
    Assert.assertEquals(Object.class, CONST_SELECTOR.classOfObject());
    Assert.assertTrue(CONST_SELECTOR.nameLookupPossibleInAdvance());
  }

  @Test
  public void testValueMatcher()
  {
    Assert.assertTrue(NULL_SELECTOR.makeValueMatcher((String) null).matches(false));
    Assert.assertFalse(NULL_SELECTOR.makeValueMatcher("douglas").matches(false));

    Assert.assertTrue(EMPTY_SELECTOR.makeValueMatcher((String) null).matches(false));
    Assert.assertFalse(EMPTY_SELECTOR.makeValueMatcher("douglas").matches(false));

    Assert.assertTrue(CONST_SELECTOR.makeValueMatcher("billy").matches(false));
    Assert.assertTrue(CONST_SELECTOR.makeValueMatcher("douglas").matches(false));
    Assert.assertFalse(CONST_SELECTOR.makeValueMatcher("debbie").matches(false));

    Assert.assertTrue(NULL_EXTRACTION_SELECTOR.makeValueMatcher("billy").matches(false));
    Assert.assertFalse(NULL_EXTRACTION_SELECTOR.makeValueMatcher((String) null).matches(false));

    Assert.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher("bill").matches(false));
    Assert.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher("doug").matches(false));
    Assert.assertFalse(CONST_EXTRACTION_SELECTOR.makeValueMatcher("billy").matches(false));

    Assert.assertTrue(NULL_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.of(DruidObjectPredicate.isNull())).matches(false));
    Assert.assertFalse(NULL_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));

    Assert.assertTrue(EMPTY_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(null)).matches(false));
    Assert.assertFalse(EMPTY_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("douglas")).matches(false));

    Assert.assertTrue(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
    Assert.assertTrue(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("douglas")).matches(false));
    Assert.assertFalse(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("debbie")).matches(false));

    Assert.assertTrue(NULL_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
    Assert.assertFalse(NULL_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(null)).matches(false));

    Assert.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("bill")).matches(false));
    Assert.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("doug")).matches(false));
    Assert.assertFalse(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
  }
}
