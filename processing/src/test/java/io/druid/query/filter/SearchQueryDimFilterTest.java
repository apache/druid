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

import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.search.search.SearchQuerySpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SearchQueryDimFilterTest
{

  @Test
  public void testGetCacheKey()
  {
    SearchQueryDimFilter searchQueryDimFilter = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        null
    );

    SearchQueryDimFilter searchQueryDimFilter2 = new SearchQueryDimFilter(
        "di",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("mvalue");
          }
        },
        null
    );
    Assert.assertFalse(Arrays.equals(searchQueryDimFilter.getCacheKey(), searchQueryDimFilter2.getCacheKey()));

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SearchQueryDimFilter searchQueryDimFilter3 = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        regexFn
    );
    Assert.assertFalse(Arrays.equals(searchQueryDimFilter.getCacheKey(), searchQueryDimFilter3.getCacheKey()));
  }

  @Test
  public void testEquals()
  {
    SearchQueryDimFilter searchQueryDimFilter = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        null
    );

    SearchQueryDimFilter searchQueryDimFilter2 = new SearchQueryDimFilter(
        "di",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("mvalue");
          }
        },
        null
    );
    Assert.assertNotEquals(searchQueryDimFilter, searchQueryDimFilter2);

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SearchQueryDimFilter searchQueryDimFilter3 = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        regexFn
    );
    Assert.assertNotEquals(searchQueryDimFilter, searchQueryDimFilter3);
  }

  @Test
  public void testHashcode()
  {
    SearchQueryDimFilter searchQueryDimFilter = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        null
    );

    SearchQueryDimFilter searchQueryDimFilter2 = new SearchQueryDimFilter(
        "di",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("mvalue");
          }
        },
        null
    );
    Assert.assertNotEquals(searchQueryDimFilter.hashCode(), searchQueryDimFilter2.hashCode());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    SearchQueryDimFilter searchQueryDimFilter3 = new SearchQueryDimFilter(
        "dim",
        new SearchQuerySpec()
        {
          @Override
          public boolean accept(String dimVal)
          {
            return false;
          }

          @Override
          public byte[] getCacheKey()
          {
            return StringUtils.toUtf8("value");
          }
        },
        regexFn
    );
    Assert.assertNotEquals(searchQueryDimFilter.hashCode(), searchQueryDimFilter3.hashCode());
  }
}
