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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

public class ConstantDimensionSelectorTest
{
  private final DimensionSelector NULL_SELECTOR = DimensionSelectorUtils.constantSelector(null);
  private final DimensionSelector CONST_SELECTOR = DimensionSelectorUtils.constantSelector("billy");
  private final DimensionSelector NULL_EXTRACTION_SELECTOR = DimensionSelectorUtils.constantSelector(
      null,
      new StringFormatExtractionFn("billy")
  );
  private final DimensionSelector CONST_EXTRACTION_SELECTOR = DimensionSelectorUtils.constantSelector(
      "billybilly",
      new SubstringDimExtractionFn(0, 5)
  );

  @Test
  public void testGetRow()
  {
    IndexedInts row = NULL_SELECTOR.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));
  }

  @Test
  public void testGetValueCardinality()
  {
    Assert.assertEquals(1, NULL_SELECTOR.getValueCardinality());
    Assert.assertEquals(1, CONST_SELECTOR.getValueCardinality());
    Assert.assertEquals(1, NULL_EXTRACTION_SELECTOR.getValueCardinality());
    Assert.assertEquals(1, CONST_EXTRACTION_SELECTOR.getValueCardinality());
  }

  @Test
  public void testLookupName()
  {
    Assert.assertEquals(null, NULL_SELECTOR.lookupName(0));
    Assert.assertEquals("billy", CONST_SELECTOR.lookupName(0));
    Assert.assertEquals("billy", NULL_EXTRACTION_SELECTOR.lookupName(0));
    Assert.assertEquals("billy", CONST_EXTRACTION_SELECTOR.lookupName(0));
  }

  @Test
  public void testLookupId()
  {
    Assert.assertEquals(0, NULL_SELECTOR.idLookup().lookupId(null));
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0 : -1, NULL_SELECTOR.idLookup().lookupId(""));
    Assert.assertEquals(-1, NULL_SELECTOR.idLookup().lookupId("billy"));
    Assert.assertEquals(-1, NULL_SELECTOR.idLookup().lookupId("bob"));

    Assert.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId(null));
    Assert.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId(""));
    Assert.assertEquals(0, CONST_SELECTOR.idLookup().lookupId("billy"));
    Assert.assertEquals(-1, CONST_SELECTOR.idLookup().lookupId("bob"));

    Assert.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId(null));
    Assert.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId(""));
    Assert.assertEquals(0, NULL_EXTRACTION_SELECTOR.idLookup().lookupId("billy"));
    Assert.assertEquals(-1, NULL_EXTRACTION_SELECTOR.idLookup().lookupId("bob"));

    Assert.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId(null));
    Assert.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId(""));
    Assert.assertEquals(0, CONST_EXTRACTION_SELECTOR.idLookup().lookupId("billy"));
    Assert.assertEquals(-1, CONST_EXTRACTION_SELECTOR.idLookup().lookupId("bob"));
  }
}
