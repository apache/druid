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

import com.google.common.collect.Sets;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.SearchQuerySpecDimExtractionFn;
import io.druid.query.search.search.FragmentSearchQuerySpec;
import io.druid.query.search.search.SearchQuerySpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 */
public class SearchQuerySpecDimExtractionFnTest
{
  private static final String[] testStrings = {
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
        Arrays.asList("to", "yo")
    );
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(spec);
    List<String> expected = Arrays.asList("Kyoto", "Tokyo", "Toyokawa", "Yorktown");
    Set<String> extracted = Sets.newHashSet();

    for (String str : testStrings) {
      String res = extractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(4, extracted.size());

    for (String str : extracted) {
      Assert.assertTrue(expected.contains(str));
    }
  }
}
