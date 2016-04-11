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

public class JavaScriptDimFilterTest
{

  @Test
  public void testGetCacheKey()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", "fn", null);
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", "mfn", null);
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("dim", "fn", regexFn);
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter3.getCacheKey()));
  }

  @Test
  public void testEquals()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", "fn", null);
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", "mfn", null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", "mfn", null);
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter2);
    Assert.assertEquals(javaScriptDimFilter2, javaScriptDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", "fn", regexFn);
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", "fn", regexFn);
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter3);
    Assert.assertEquals(javaScriptDimFilter4, javaScriptDimFilter5);
  }

  @Test
  public void testHashcode()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", "fn", null);
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", "mfn", null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", "mfn", null);
    Assert.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter2.hashCode());
    Assert.assertEquals(javaScriptDimFilter2.hashCode(), javaScriptDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", "fn", regexFn);
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", "fn", regexFn);
    Assert.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter3.hashCode());
    Assert.assertEquals(javaScriptDimFilter4.hashCode(), javaScriptDimFilter5.hashCode());
  }
}
