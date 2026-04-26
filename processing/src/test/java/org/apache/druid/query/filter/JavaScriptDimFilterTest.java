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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class JavaScriptDimFilterTest
{
  private static final String FN1 = "function(x) { return x }";
  private static final String FN2 = "function(x) { return x + x }";

  @Test
  public void testGetCacheKey()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assertions.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    Assertions.assertFalse(Arrays.equals(javaScriptDimFilter.getCacheKey(), javaScriptDimFilter3.getCacheKey()));
  }

  @Test
  public void testEquals()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assertions.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter2);
    Assertions.assertEquals(javaScriptDimFilter2, javaScriptDimFilter3);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    Assertions.assertNotEquals(javaScriptDimFilter, javaScriptDimFilter3);
    Assertions.assertEquals(javaScriptDimFilter4, javaScriptDimFilter5);
  }

  @Test
  public void testHashcode()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter("di", FN2, null, JavaScriptConfig.getEnabledInstance());
    Assertions.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter2.hashCode());
    Assertions.assertEquals(javaScriptDimFilter2.hashCode(), javaScriptDimFilter3.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    JavaScriptDimFilter javaScriptDimFilter4 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    JavaScriptDimFilter javaScriptDimFilter5 = new JavaScriptDimFilter("dim", FN1, regexFn, JavaScriptConfig.getEnabledInstance());
    Assertions.assertNotEquals(javaScriptDimFilter.hashCode(), javaScriptDimFilter3.hashCode());
    Assertions.assertEquals(javaScriptDimFilter4.hashCode(), javaScriptDimFilter5.hashCode());
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
    Assertions.assertInstanceOf(JavaScriptFilter.class, filter);
  }

  @Test
  public void testToFilterNotAllowed()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, new JavaScriptConfig(false));

    IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        javaScriptDimFilter::toFilter
    );
    Assertions.assertTrue(e.getMessage().contains("JavaScript is disabled"));
  }

  @Test
  public void testGetRequiredColumns()
  {
    JavaScriptDimFilter javaScriptDimFilter = new JavaScriptDimFilter("dim", FN1, null, new JavaScriptConfig(false));
    Assertions.assertEquals(javaScriptDimFilter.getRequiredColumns(), Sets.newHashSet("dim"));
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
    Assertions.assertTrue(javaScriptDimFilter.getPredicateFactory().applyObject("123").matches(false));
    Assertions.assertTrue(javaScriptDimFilter.getPredicateFactory().applyObject("456").matches(false));
    Assertions.assertFalse(javaScriptDimFilter.getPredicateFactory().applyObject("789").matches(false));

    // test for return java.lang.Boolean
    JavaScriptDimFilter javaScriptDimFilter1 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return ['123', '456'].includes(id) == true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assertions.assertTrue(javaScriptDimFilter1.getPredicateFactory().applyObject("123").matches(false));
    Assertions.assertTrue(javaScriptDimFilter1.getPredicateFactory().applyObject("456").matches(false));
    Assertions.assertFalse(javaScriptDimFilter1.getPredicateFactory().applyObject("789").matches(false));

    // test for return other type
    JavaScriptDimFilter javaScriptDimFilter2 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return 'word'; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assertions.assertTrue(javaScriptDimFilter2.getPredicateFactory().applyObject("123").matches(false));

    // test for return null
    JavaScriptDimFilter javaScriptDimFilter3 = new JavaScriptDimFilter(
            "dim",
            "function(id) { return null; }",
            null,
            JavaScriptConfig.getEnabledInstance()
    );
    Assertions.assertFalse(javaScriptDimFilter3.getPredicateFactory().applyObject("123").matches(false));
  }
}
