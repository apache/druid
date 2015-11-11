/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex);
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
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex);
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
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex);
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
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex);
    // no match, map empty input value to null
    Assert.assertEquals(null, extractionFn.apply(""));
    // null value, returns null
    Assert.assertEquals(null, extractionFn.apply(null));
    // empty match, map empty result to null
    Assert.assertEquals(null, extractionFn.apply("/a/b"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"regex\", \"expr\" : \".(...)?\" }";
    RegexDimExtractionFn extractionFn = (RegexDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals(".(...)?", extractionFn.getExpr());

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
