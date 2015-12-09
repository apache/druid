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

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class RegexDimExtractionFnTest
{
  private static final String[] paths = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private static final String[] testStrings = {
      "apple",
      "awesome",
      "asylum",
      "business",
      "be",
      "cool"
  };

  @Test
  public void testPathExtraction()
  {
    String regex = "/([^/]+)/";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null, false);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(extractionFn.apply(path));
    }

    Assert.assertEquals(2, extracted.size());
    Assert.assertTrue(extracted.contains("druid"));
    Assert.assertTrue(extracted.contains("dash"));
  }

  @Test
  public void testDeeperPathExtraction()
  {
    String regex = "^/([^/]+/[^/]+)(/|$)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null, false);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(extractionFn.apply(path));
    }

    Assert.assertEquals(4, extracted.size());
    Assert.assertTrue(extracted.contains("druid/prod"));
    Assert.assertTrue(extracted.contains("druid/demo"));
    Assert.assertTrue(extracted.contains("dash/aloe"));
    Assert.assertTrue(extracted.contains("dash/baloo"));
  }

  @Test
  public void testStringExtraction()
  {
    String regex = "(.)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null, false);
    Set<String> extracted = Sets.newHashSet();

    for (String testString : testStrings) {
      extracted.add(extractionFn.apply(testString));
    }

    Assert.assertEquals(3, extracted.size());
    Assert.assertTrue(extracted.contains("a"));
    Assert.assertTrue(extracted.contains("b"));
    Assert.assertTrue(extracted.contains("c"));
  }


  @Test
  public void testNullAndEmpty()
  {
    String regex = "(.*)/.*/.*";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null, false);
    // no match, map empty input value to null
    Assert.assertEquals(null, extractionFn.apply(""));
    // null value, returns null
    Assert.assertEquals(null, extractionFn.apply(null));
    // empty match, map empty result to null
    Assert.assertEquals(null, extractionFn.apply("/a/b"));
  }

  @Test
  public void testMissingValueReplacement()
  {
    String regex = "(a\\w*)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, true, "foobar", false);
    Set<String> extracted = Sets.newHashSet();

    for (String testString : testStrings) {
      extracted.add(extractionFn.apply(testString));
    }

    Assert.assertEquals(4, extracted.size());
    Assert.assertTrue(extracted.contains("apple"));
    Assert.assertTrue(extracted.contains("awesome"));
    Assert.assertTrue(extracted.contains("asylum"));
    Assert.assertTrue(extracted.contains("foobar"));

    byte[] cacheKey = extractionFn.getCacheKey();
    Assert.assertEquals(17, cacheKey.length);

    ExtractionFn nullExtractionFn = new RegexDimExtractionFn(regex, true, null, false);
    Set<String> extracted2 = Sets.newHashSet();

    for (String testString : testStrings) {
      extracted2.add(nullExtractionFn.apply(testString));
    }

    Assert.assertEquals(4, extracted2.size());
    Assert.assertTrue(extracted2.contains("apple"));
    Assert.assertTrue(extracted2.contains("awesome"));
    Assert.assertTrue(extracted2.contains("asylum"));
    Assert.assertTrue(extracted2.contains(null));

    cacheKey = nullExtractionFn.getCacheKey();
    Assert.assertEquals(11, cacheKey.length);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"regex\", \"expr\" : \".(...)?\" , \"injective\":true, " +
                        "\"replaceMissingValues\": true, \"replaceMissingValuesWith\":\"foobar\"}";
    RegexDimExtractionFn extractionFn = (RegexDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals(".(...)?", extractionFn.getExpr());
    Assert.assertTrue(extractionFn.isReplaceMissingValues());
    Assert.assertEquals("foobar", extractionFn.getReplaceMissingValuesWith());
    Assert.assertTrue(extractionFn.isInjective());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
