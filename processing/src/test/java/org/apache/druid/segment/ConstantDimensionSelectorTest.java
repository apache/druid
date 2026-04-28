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

import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConstantDimensionSelectorTest extends InitializedNullHandlingTest
{
  private final DimensionSelector NULL_SELECTOR = DimensionSelector.constant(null);
  private final DimensionSelector CONST_SELECTOR = DimensionSelector.constant("billy");
  private final DimensionSelector NULL_EXTRACTION_SELECTOR = DimensionSelector.constant(
      null,
      new StringFormatExtractionFn("billy")
  );
  private final DimensionSelector CONST_EXTRACTION_SELECTOR = DimensionSelector.constant(
      "billybilly",
      new SubstringDimExtractionFn(0, 5)
  );

  @Test
  public void testGetRow()
  {
    IndexedInts row = NULL_SELECTOR.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(0, row.get(0));
  }

  @Test
  public void testGetValueCardinality()
  {
    Assertions.assertEquals(1, NULL_SELECTOR.getValueCardinality());
    Assertions.assertEquals(1, CONST_SELECTOR.getValueCardinality());
    Assertions.assertEquals(1, NULL_EXTRACTION_SELECTOR.getValueCardinality());
    Assertions.assertEquals(1, CONST_EXTRACTION_SELECTOR.getValueCardinality());
  }

  @Test
  public void testLookupName()
  {
    Assertions.assertNull(NULL_SELECTOR.lookupName(0));
    Assertions.assertEquals("billy", CONST_SELECTOR.lookupName(0));
    Assertions.assertEquals("billy", NULL_EXTRACTION_SELECTOR.lookupName(0));
    Assertions.assertEquals("billy", CONST_EXTRACTION_SELECTOR.lookupName(0));
  }

  @Test
  public void testLookupId()
  {
    Assertions.assertEquals(0, NULL_SELECTOR.idLookup().lookupId(null));
    Assertions.assertEquals(-1, NULL_SELECTOR.idLookup().lookupId(""));
    Assertions.assertEquals(-1, NULL_SELECTOR.idLookup().lookupId("billy"));
    Assertions.assertEquals(-1, NULL_SELECTOR.idLookup().lookupId("bob"));

    Assertions.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId(null));
    Assertions.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId(""));
    Assertions.assertEquals(0, CONST_SELECTOR.idLookup().lookupId("billy"));
    Assertions.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId("bob"));

    Assertions.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId(null));
    Assertions.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId(""));
    Assertions.assertEquals(0, NULL_EXTRACTION_SELECTOR.idLookup().lookupId("billy"));
    Assertions.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId("bob"));

    Assertions.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId(null));
    Assertions.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId(""));
    Assertions.assertEquals(0, CONST_EXTRACTION_SELECTOR.idLookup().lookupId("billy"));
    Assertions.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId("bob"));
  }

  @Test
  public void testValueMatcherPredicates()
  {
    DruidPredicateFactory nullUnkown = StringPredicateDruidPredicateFactory.of(
        value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.TRUE
    );
    ValueMatcher matcher = NULL_SELECTOR.makeValueMatcher(nullUnkown);
    Assertions.assertFalse(matcher.matches(false));
    Assertions.assertTrue(matcher.matches(true));

    DruidPredicateFactory notUnknown = StringPredicateDruidPredicateFactory.of(DruidObjectPredicate.notNull());
    matcher = NULL_SELECTOR.makeValueMatcher(notUnknown);
    Assertions.assertFalse(matcher.matches(false));
    Assertions.assertFalse(matcher.matches(true));
  }
}
