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

import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.segment.filter.JavaScriptFilter;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

public class JavaScriptDimFilterTest
{
  private static final String FN1 = "function(x) { return x }";
  private static final String FN2 = "function(x) { return x + x }";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetCacheKey()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getDefault());
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getDefault());
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter3.getCacheKey()));
  }

  @Test
  public void testEquals()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getDefault());
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter2);
    Assert.assertEquals(javaScriptDimFilter2, javaScriptDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getDefault());
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter3);
    Assert.assertEquals(javaScriptDimFilter4, javaScriptDimFilter5);
  }

  @Test
  public void testHashcode()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getDefault());
    Assert.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter2.hashCode());
    Assert.assertEquals(javaScriptDimFilter2.hashCode(), javaScriptDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getDefault());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getDefault());
    Assert.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter3.hashCode());
    Assert.assertEquals(javaScriptDimFilter4.hashCode(), javaScriptDimFilter5.hashCode());
  }

  @Test
  public void testToFilter()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter(
        "dim",
        "function(x) { return true; }",
        null,
        JavaScriptConfig.getDefault()
    );
    final Filter filter = javaScriptDimFilter.toFilter();
    Assert.assertThat(filter, CoreMatchers.instanceOf(JavaScriptFilter.class));
  }

  @Test
  public void testToFilterNotAllowed()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, new JavaScriptConfig(true));

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    javaScriptDimFilter.toFilter();
    Assert.assertTrue(false);
  }
}
