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

package io.druid.query.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchSortSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 */
public class SearchSortSpecTest
{
  @Test
  public void testLexicographicComparator()
  {
    SearchHit hit1 = new SearchHit("test", "apple");
    SearchHit hit2 = new SearchHit("test", "banana");
    SearchHit hit3 = new SearchHit("test", "banana");

    SearchSortSpec spec = new SearchSortSpec(StringComparators.LEXICOGRAPHIC);

    Assert.assertTrue(spec.getComparator().compare(hit2, hit3) == 0);
    Assert.assertTrue(spec.getComparator().compare(hit2, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit1, hit3) < 0);
  }

  @Test
  public void testAlphanumericComparator()
  {
    SearchSortSpec spec = new SearchSortSpec(StringComparators.ALPHANUMERIC);

    SearchHit hit1 = new SearchHit("test", "a100");
    SearchHit hit2 = new SearchHit("test", "a9");
    SearchHit hit3 = new SearchHit("test", "b0");

    Assert.assertTrue(spec.getComparator().compare(hit1, hit2) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit2) > 0);
  }

  @Test
  public void testNumericComparator()
  {
    SearchSortSpec spec = new SearchSortSpec(StringComparators.NUMERIC);

    SearchHit hit1 = new SearchHit("test", "1001001.12412");
    SearchHit hit2 = new SearchHit("test", "-1421");
    SearchHit hit3 = new SearchHit("test", "not-numeric-at-all");

    SearchHit hit4 = new SearchHit("best", "1001001.12412");


    Assert.assertTrue(spec.getComparator().compare(hit1, hit2) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit1) < 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit2) < 0);

    Assert.assertTrue(spec.getComparator().compare(hit1, hit4) > 0);
  }

  @Test
  public void testStrlenComparator()
  {
    SearchSortSpec spec = new SearchSortSpec(StringComparators.LEXICOGRAPHIC);

    SearchHit hit1 = new SearchHit("test", "apple");
    SearchHit hit2 = new SearchHit("test", "banana");
    SearchHit hit3 = new SearchHit("test", "orange");

    Assert.assertTrue(spec.getComparator().compare(hit1, hit2) < 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit3, hit2) > 0);

    Assert.assertTrue(spec.getComparator().compare(hit1, hit1) == 0);
  }


  @Test
  public void testSerde() throws IOException
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    SearchSortSpec spec = new SearchSortSpec(StringComparators.ALPHANUMERIC);

    String expectJsonSpec = "{\"type\":{\"type\":\"alphanumeric\"}}";
    String jsonSpec = jsonMapper.writeValueAsString(spec);
    Assert.assertEquals(expectJsonSpec, jsonSpec);
    Assert.assertEquals(spec, jsonMapper.readValue(jsonSpec, SearchSortSpec.class));

    String expectJsonSpec2 = "{\"type\":\"alphanumeric\"}";
    Assert.assertEquals(spec, jsonMapper.readValue(expectJsonSpec2, SearchSortSpec.class));
  }
}
