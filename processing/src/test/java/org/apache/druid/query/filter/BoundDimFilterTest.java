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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class BoundDimFilterTest
{
  private static final ExtractionFn EXTRACTION_FN = new RegexDimExtractionFn(".*", false, null);

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {

    return ImmutableList.of(
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, null, null, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, true, false, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, null, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", null, "15", null, true, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, null, null, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", null, true, null, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, true, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, true, false, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", null, "15", null, true, true, EXTRACTION_FN,
                                        StringComparators.ALPHANUMERIC)}
    );
  }

  private final BoundDimFilter boundDimFilter;

  public BoundDimFilterTest(BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
  }

  @Test
  public void testSerDesBoundFilter() throws IOException
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    ObjectMapper mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
    String serBetweenDimFilter = mapper.writeValueAsString(boundDimFilter);
    BoundDimFilter actualBoundDimFilter = mapper.readerFor(DimFilter.class).readValue(serBetweenDimFilter);
    Assert.assertEquals(boundDimFilter, actualBoundDimFilter);
  }

  @Test
  public void testGetCacheKey()
  {
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterCopy = new BoundDimFilter("dimension", "12", "15", false, false, true, null, StringComparators.ALPHANUMERIC);
    Assert.assertArrayEquals(boundDimFilter.getCacheKey(), boundDimFilterCopy.getCacheKey());
    BoundDimFilter anotherBoundDimFilter = new BoundDimFilter("dimension", "12", "15", true, null, false, null, StringComparators.LEXICOGRAPHIC);
    Assert.assertFalse(Arrays.equals(anotherBoundDimFilter.getCacheKey(), boundDimFilter.getCacheKey()));

    BoundDimFilter boundDimFilterWithExtract = new BoundDimFilter("dimension", "12", "15", null, null, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterWithExtractCopy = new BoundDimFilter("dimension", "12", "15", false, false, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);
    Assert.assertFalse(Arrays.equals(boundDimFilter.getCacheKey(), boundDimFilterWithExtract.getCacheKey()));
    Assert.assertArrayEquals(boundDimFilterWithExtract.getCacheKey(), boundDimFilterWithExtractCopy.getCacheKey());
  }

  @Test
  public void testHashCode()
  {
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterWithExtract = new BoundDimFilter("dimension", "12", "15", null, null, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);

    Assert.assertNotEquals(boundDimFilter.hashCode(), boundDimFilterWithExtract.hashCode());
  }

  @Test
  public void testGetRequiredColumns()
  {
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    Assert.assertEquals(boundDimFilter.getRequiredColumns(), Sets.newHashSet("dimension"));
  }
}
