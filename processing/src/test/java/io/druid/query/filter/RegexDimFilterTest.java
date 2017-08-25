/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.filter;

import io.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RegexDimFilterTest
{

  @Test
  public void testGetCacheKey()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    Assert.assertFalse(Arrays.equals(regexDimFilter.getCacheKey(), regexDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("dim", "reg", regexFn);
    Assert.assertFalse(Arrays.equals(regexDimFilter.getCacheKey(), regexDimFilter3.getCacheKey()));

  }

  @Test
  public void testEquals()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("di", "mreg", null);

    Assert.assertNotEquals(regexDimFilter, regexDimFilter2);
    Assert.assertEquals(regexDimFilter2, regexDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter4 = new RegexDimFilter("dim", "reg", regexFn);
    RegexDimFilter regexDimFilter5 = new RegexDimFilter("dim", "reg", regexFn);
    Assert.assertNotEquals(regexDimFilter, regexDimFilter4);
    Assert.assertEquals(regexDimFilter4, regexDimFilter5);

  }

  @Test
  public void testHashcode()
  {
    RegexDimFilter regexDimFilter = new RegexDimFilter("dim", "reg", null);
    RegexDimFilter regexDimFilter2 = new RegexDimFilter("di", "mreg", null);
    RegexDimFilter regexDimFilter3 = new RegexDimFilter("di", "mreg", null);

    Assert.assertNotEquals(regexDimFilter.hashCode(), regexDimFilter2.hashCode());
    Assert.assertEquals(regexDimFilter2.hashCode(), regexDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    RegexDimFilter regexDimFilter4 = new RegexDimFilter("dim", "reg", regexFn);
    RegexDimFilter regexDimFilter5 = new RegexDimFilter("dim", "reg", regexFn);
    Assert.assertNotEquals(regexDimFilter.hashCode(), regexDimFilter4.hashCode());
    Assert.assertEquals(regexDimFilter4.hashCode(), regexDimFilter5.hashCode());
  }

}
