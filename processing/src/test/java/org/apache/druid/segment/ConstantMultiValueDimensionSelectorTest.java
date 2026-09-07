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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(0, row.get(0));

    row = EMPTY_SELECTOR.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(0, row.get(0));

    row = SINGLE_SELECTOR.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(0, row.get(0));

    row = CONST_SELECTOR.getRow();
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals(0, row.get(0));
    Assertions.assertEquals(1, row.get(1));

    row = NULL_EXTRACTION_SELECTOR.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(0, row.get(0));

    row = CONST_EXTRACTION_SELECTOR.getRow();
    Assertions.assertEquals(3, row.size());
    Assertions.assertEquals(0, row.get(0));
    Assertions.assertEquals(1, row.get(1));
    Assertions.assertEquals(2, row.get(2));
  }

  @Test
  public void testLookupName()
  {
    Assertions.assertNull(NULL_SELECTOR.lookupName(0));

    Assertions.assertNull(EMPTY_SELECTOR.lookupName(0));

    Assertions.assertEquals("billy", CONST_SELECTOR.lookupName(0));
    Assertions.assertEquals("douglas", CONST_SELECTOR.lookupName(1));

    Assertions.assertEquals("billy", NULL_EXTRACTION_SELECTOR.lookupName(0));

    Assertions.assertEquals("bill", CONST_EXTRACTION_SELECTOR.lookupName(0));
    Assertions.assertEquals("doug", CONST_EXTRACTION_SELECTOR.lookupName(1));
    Assertions.assertEquals("bill", CONST_EXTRACTION_SELECTOR.lookupName(2));
  }

  @Test
  public void testGetObject()
  {
    Assertions.assertNull(NULL_SELECTOR.lookupName(0));

    Assertions.assertNull(EMPTY_SELECTOR.lookupName(0));

    Assertions.assertEquals("billy", SINGLE_SELECTOR.getObject());
    Assertions.assertEquals(ImmutableList.of("billy", "douglas"), CONST_SELECTOR.getObject());

    Assertions.assertEquals("billy", NULL_EXTRACTION_SELECTOR.getObject());

    Assertions.assertEquals(ImmutableList.of("bill", "doug", "bill"), CONST_EXTRACTION_SELECTOR.getObject());
  }

  @Test
  public void testCoverage()
  {
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, CONST_SELECTOR.getValueCardinality());
    Assertions.assertNull(CONST_SELECTOR.idLookup());
    Assertions.assertEquals(Object.class, CONST_SELECTOR.classOfObject());
    Assertions.assertTrue(CONST_SELECTOR.nameLookupPossibleInAdvance());
  }

  @Test
  public void testValueMatcher()
  {
    Assertions.assertTrue(NULL_SELECTOR.makeValueMatcher((String) null).matches(false));
    Assertions.assertFalse(NULL_SELECTOR.makeValueMatcher("douglas").matches(false));

    Assertions.assertTrue(EMPTY_SELECTOR.makeValueMatcher((String) null).matches(false));
    Assertions.assertFalse(EMPTY_SELECTOR.makeValueMatcher("douglas").matches(false));

    Assertions.assertTrue(CONST_SELECTOR.makeValueMatcher("billy").matches(false));
    Assertions.assertTrue(CONST_SELECTOR.makeValueMatcher("douglas").matches(false));
    Assertions.assertFalse(CONST_SELECTOR.makeValueMatcher("debbie").matches(false));

    Assertions.assertTrue(NULL_EXTRACTION_SELECTOR.makeValueMatcher("billy").matches(false));
    Assertions.assertFalse(NULL_EXTRACTION_SELECTOR.makeValueMatcher((String) null).matches(false));

    Assertions.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher("bill").matches(false));
    Assertions.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher("doug").matches(false));
    Assertions.assertFalse(CONST_EXTRACTION_SELECTOR.makeValueMatcher("billy").matches(false));

    Assertions.assertTrue(NULL_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.of(DruidObjectPredicate.isNull())).matches(false));
    Assertions.assertFalse(NULL_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));

    Assertions.assertTrue(EMPTY_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(null)).matches(false));
    Assertions.assertFalse(EMPTY_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("douglas")).matches(false));

    Assertions.assertTrue(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
    Assertions.assertTrue(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("douglas")).matches(false));
    Assertions.assertFalse(CONST_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("debbie")).matches(false));

    Assertions.assertTrue(NULL_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
    Assertions.assertFalse(NULL_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(null)).matches(false));

    Assertions.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("bill")).matches(false));
    Assertions.assertTrue(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("doug")).matches(false));
    Assertions.assertFalse(CONST_EXTRACTION_SELECTOR.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("billy")).matches(false));
  }
}
