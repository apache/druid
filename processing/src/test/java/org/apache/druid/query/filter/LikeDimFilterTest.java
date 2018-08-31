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
import com.google.common.collect.Sets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class LikeDimFilterTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = objectMapper.readValue(objectMapper.writeValueAsString(filter), DimFilter.class);
    Assert.assertEquals(filter, filter2);
  }

  @Test
  public void testGetCacheKey()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertArrayEquals(filter.getCacheKey(), filter2.getCacheKey());
    Assert.assertFalse(Arrays.equals(filter.getCacheKey(), filter3.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter, filter2);
    Assert.assertNotEquals(filter, filter3);
    Assert.assertEquals(filter.hashCode(), filter2.hashCode());
    Assert.assertNotEquals(filter.hashCode(), filter3.hashCode());
  }

  @Test
  public void testGetRequiredColumns()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter.getRequiredColumns(), Sets.newHashSet("foo"));
  }
}
