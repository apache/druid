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
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.segment.filter.JavaScriptFilter;
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
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    Assert.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter3.getCacheKey()));
  }

  @Test
  public void testEquals()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter2);
    Assert.assertEquals(javaScriptDimFilter2, javaScriptDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    Assert.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter3);
    Assert.assertEquals(javaScriptDimFilter4, javaScriptDimFilter5);
  }

  @Test
  public void testHashcode()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assert.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter2.hashCode());
    Assert.assertEquals(javaScriptDimFilter2.hashCode(), javaScriptDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
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
        JavaScriptConfig.getEnabledInstance()
    );
    final Filter filter = javaScriptDimFilter.toFilter();
    Assert.assertThat(filter, CoreMatchers.instanceOf(JavaScriptFilter.class));
  }

  @Test
  public void testToFilterNotAllowed()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, new JavaScriptConfig(false));

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    javaScriptDimFilter.toFilter();
    Assert.assertTrue(false);
  }

  @Test
  public void testGetRequiredColumns()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, new JavaScriptConfig(false));
    Assert.assertEquals(javaScriptDimFilter.getRequiredColumns(), Sets.newHashSet("dim"));
  }

  @Test
  public void testPredicateFactoryApplyObject()
  {
    // test for return org.mozilla.javascript.NativeBoolean
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter(
            "dim",
            "function(id) { return ['123', '456'].includes(id); }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assert.assertTrue(javaScriptDimFilter.getPredicateFactory().applyObject("123").matches(false));
    Assert.assertTrue(javaScriptDimFilter.getPredicateFactory().applyObject("456").matches(false));
    Assert.assertFalse(javaScriptDimFilter.getPredicateFactory().applyObject("789").matches(false));

    // test for return java.lang.Boolean
    JavaScriptDimFilter javaScriptDimFilter1 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return ['123', '456'].includes(id) == true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assert.assertTrue(javaScriptDimFilter1.getPredicateFactory().applyObject("123").matches(false));
    Assert.assertTrue(javaScriptDimFilter1.getPredicateFactory().applyObject("456").matches(false));
    Assert.assertFalse(javaScriptDimFilter1.getPredicateFactory().applyObject("789").matches(false));

    // test for return other type
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return 'word'; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assert.assertTrue(javaScriptDimFilter2.getPredicateFactory().applyObject("123").matches(false));

    // test for return null
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return null; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assert.assertFalse(javaScriptDimFilter3.getPredicateFactory().applyObject("123").matches(false));
  }
}
