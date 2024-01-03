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

package org.apache.druid.query.filter;

import com.google.common.collect.Sets;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class SelectorDimFilterTest
{
  @Test
  public void testGetCacheKey()
  {
    SelectorDimFilter selectorDimFilter = new SelectorDimFilter("abc", "d", null);
    SelectorDimFilter selectorDimFilter2 = new SelectorDimFilter("ab", "cd", null);
    Assert.assertFalse(Arrays.equals(selectorDimFilter.getCacheKey(), selectorDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SelectorDimFilter selectorDimFilter3 = new SelectorDimFilter("abc", "d", regexFn);
    Assert.assertFalse(Arrays.equals(selectorDimFilter.getCacheKey(), selectorDimFilter3.getCacheKey()));
  }

  @Test
  public void testToString()
  {
    SelectorDimFilter selectorDimFilter = new SelectorDimFilter("abc", "d", null);
    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SelectorDimFilter selectorDimFilter2 = new SelectorDimFilter("abc", "d", regexFn);

    Assert.assertEquals("abc = d", selectorDimFilter.toString());
    Assert.assertEquals("regex(/.*/, 1)(abc) = d", selectorDimFilter2.toString());
  }

  @Test
  public void testHashCode()
  {
    SelectorDimFilter selectorDimFilter = new SelectorDimFilter("abc", "d", null);
    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SelectorDimFilter selectorDimFilter2 = new SelectorDimFilter("abc", "d", regexFn);

    Assert.assertNotEquals(selectorDimFilter.hashCode(), selectorDimFilter2.hashCode());
  }

  @Test
  public void testSimpleOptimize()
  {
    SelectorDimFilter selectorDimFilter = new SelectorDimFilter("abc", "d", null);
    DimFilter filter = new AndDimFilter(
        Collections.singletonList(
            new OrDimFilter(
                Collections.singletonList(
                    new AndDimFilter(Arrays.asList(selectorDimFilter, null))
                )
            )
        )
    );
    Assert.assertEquals(selectorDimFilter, filter.optimize(false));
    Assert.assertEquals(selectorDimFilter, filter.optimize(true));
  }

  @Test
  public void testGetRequiredColumns()
  {
    SelectorDimFilter selectorDimFilter = new SelectorDimFilter("abc", "d", null);
    Assert.assertEquals(selectorDimFilter.getRequiredColumns(), Sets.newHashSet("abc"));
  }
}
