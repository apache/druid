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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.search.FragmentSearchQuerySpec;
import org.apache.druid.query.search.SearchQuerySpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class SearchQuerySpecDimExtractionFnTest
{
  private static final String[] TEST_STRINGS = {
      "Kyoto",
      "Calgary",
      "Tokyo",
      "Stockholm",
      "Toyokawa",
      "Pretoria",
      "Yorktown",
      "Ontario"
  };

  @Test
  public void testExtraction()
  {
    SearchQuerySpec spec = new FragmentSearchQuerySpec(
        Arrays.asList("tO", "yO")
    );
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(spec);
    List<String> expected = ImmutableList.of("Kyoto", "Tokyo", "Toyokawa", "Yorktown");
    List<String> extracted = new ArrayList<>();

    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(expected, extracted);
  }

  @Test
  public void testCaseSensitiveExtraction()
  {
    SearchQuerySpec spec = new FragmentSearchQuerySpec(
        Arrays.asList("to", "yo"),
        true
    );
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(spec);
    List<String> expected = ImmutableList.of("Kyoto");
    List<String> extracted = new ArrayList<>();

    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(expected, extracted);
  }

  @Test
  public void testCaseSensitiveExtraction2()
  {
    SearchQuerySpec spec = new FragmentSearchQuerySpec(
        Arrays.asList("To", "yo"),
        true
    );
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(spec);
    List<String> expected = ImmutableList.of("Tokyo", "Toyokawa");
    List<String> extracted = new ArrayList<>();

    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(expected, extracted);
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    SearchQuerySpec spec = new FragmentSearchQuerySpec(
        Arrays.asList("to", "yo"),
        true
    );
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(spec);

    ExtractionFn extractionFn2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(extractionFn),
        ExtractionFn.class
    );
    FragmentSearchQuerySpec spec2 = (FragmentSearchQuerySpec) ((SearchQuerySpecDimExtractionFn) extractionFn2).getSearchQuerySpec();
    Assert.assertEquals(extractionFn, extractionFn2);
    Assert.assertEquals(true, spec2.isCaseSensitive());
    Assert.assertEquals(ImmutableList.of("to", "yo"), spec2.getValues());
  }
}
