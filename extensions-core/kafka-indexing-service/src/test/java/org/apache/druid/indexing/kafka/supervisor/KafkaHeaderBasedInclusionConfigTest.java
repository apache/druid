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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class KafkaHeaderBasedInclusionConfigTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @BeforeClass
  public static void setUpStatic()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Test
  public void testInFilterSingleValue()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedInclusionConfig filter = new KafkaHeaderBasedInclusionConfig(dimFilter, null, null);

    Assert.assertEquals(dimFilter, filter.getFilter());
    Assert.assertEquals("UTF-8", filter.getEncoding());
    Assert.assertEquals(10_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testInFilterMultipleValues()
  {
    InDimFilter dimFilter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedInclusionConfig filter = new KafkaHeaderBasedInclusionConfig(dimFilter, "ISO-8859-1", null);

    Assert.assertEquals(dimFilter, filter.getFilter());
    Assert.assertEquals("ISO-8859-1", filter.getEncoding());
    Assert.assertEquals(10_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testInFilterWithCustomCacheSize()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedInclusionConfig filter = new KafkaHeaderBasedInclusionConfig(dimFilter, null, 50_000);

    Assert.assertEquals(dimFilter, filter.getFilter());
    Assert.assertEquals("UTF-8", filter.getEncoding());
    Assert.assertEquals(50_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testSelectorFilterRejected()
  {
    SelectorDimFilter dimFilter = new SelectorDimFilter("environment", "production", null);
    try {
      new KafkaHeaderBasedInclusionConfig(dimFilter, null, null);
      Assert.fail("Expected DruidException for SelectorDimFilter");
    }
    catch (DruidException e) {
      Assert.assertTrue("Should mention unsupported filter type", e.getMessage().contains("Unsupported filter type"));
      Assert.assertTrue("Should mention SelectorDimFilter", e.getMessage().contains("SelectorDimFilter"));
    }
  }

  @Test
  public void testAndFilterRejected()
  {
    SelectorDimFilter envFilter = new SelectorDimFilter("environment", "production", null);
    SelectorDimFilter serviceFilter = new SelectorDimFilter("service", "user-service", null);
    AndDimFilter andFilter = new AndDimFilter(Arrays.asList(envFilter, serviceFilter));
    try {
      new KafkaHeaderBasedInclusionConfig(andFilter, null, null);
      Assert.fail("Expected DruidException for AndDimFilter");
    }
    catch (DruidException e) {
      Assert.assertTrue("Should mention unsupported filter type", e.getMessage().contains("Unsupported filter type"));
      Assert.assertTrue("Should mention AndDimFilter", e.getMessage().contains("AndDimFilter"));
    }
  }

  @Test
  public void testNotFilterRejected()
  {
    SelectorDimFilter debugFilter = new SelectorDimFilter("debug-mode", "true", null);
    NotDimFilter notFilter = new NotDimFilter(debugFilter);
    try {
      new KafkaHeaderBasedInclusionConfig(notFilter, null, null);
      Assert.fail("Expected DruidException for NotDimFilter");
    }
    catch (DruidException e) {
      Assert.assertTrue("Should mention unsupported filter type", e.getMessage().contains("Unsupported filter type"));
      Assert.assertTrue("Should mention NotDimFilter", e.getMessage().contains("NotDimFilter"));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullFilter()
  {
    new KafkaHeaderBasedInclusionConfig(null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEncoding()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    new KafkaHeaderBasedInclusionConfig(dimFilter, "INVALID-ENCODING", null);
  }

  @Test
  public void testSerialization() throws Exception
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedInclusionConfig originalFilter = new KafkaHeaderBasedInclusionConfig(dimFilter, "UTF-16", null);

    // Serialize to JSON
    String json = objectMapper.writeValueAsString(originalFilter);

    // Deserialize back
    KafkaHeaderBasedInclusionConfig deserializedFilter = objectMapper.readValue(json, KafkaHeaderBasedInclusionConfig.class);

    Assert.assertEquals(originalFilter.getFilter(), deserializedFilter.getFilter());
    Assert.assertEquals(originalFilter.getEncoding(), deserializedFilter.getEncoding());
    Assert.assertEquals(originalFilter.getStringDecodingCacheSize(), deserializedFilter.getStringDecodingCacheSize());
  }

  @Test
  public void testEquals()
  {
    InDimFilter dimFilter1 = new InDimFilter("environment", Collections.singletonList("production"), null);
    InDimFilter dimFilter2 = new InDimFilter("environment", Collections.singletonList("production"), null);
    InDimFilter dimFilter3 = new InDimFilter("environment", Collections.singletonList("staging"), null);

    KafkaHeaderBasedInclusionConfig filter1 = new KafkaHeaderBasedInclusionConfig(dimFilter1, "UTF-8", null);
    KafkaHeaderBasedInclusionConfig filter2 = new KafkaHeaderBasedInclusionConfig(dimFilter2, "UTF-8", null);
    KafkaHeaderBasedInclusionConfig filter3 = new KafkaHeaderBasedInclusionConfig(dimFilter3, "UTF-8", null);
    KafkaHeaderBasedInclusionConfig filter4 = new KafkaHeaderBasedInclusionConfig(dimFilter1, "UTF-16", null);
    KafkaHeaderBasedInclusionConfig filter5 = new KafkaHeaderBasedInclusionConfig(dimFilter1, "UTF-8", 5000);

    Assert.assertEquals(filter1, filter2);
    Assert.assertNotEquals(filter1, filter3);
    Assert.assertNotEquals(filter1, filter4);
    Assert.assertNotEquals(filter1, filter5); // Different cache size
    Assert.assertNotEquals(filter1, null);
    Assert.assertNotEquals(filter1, "string");
  }

  @Test
  public void testToString()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedInclusionConfig filter = new KafkaHeaderBasedInclusionConfig(dimFilter, "UTF-8", null);

    String toString = filter.toString();
    Assert.assertTrue(toString.contains("KafkaheaderBasedInclusionConfig"));
    Assert.assertTrue(toString.contains("filter="));
    Assert.assertTrue(toString.contains("encoding='UTF-8'"));
    Assert.assertTrue(toString.contains("stringDecodingCacheSize=10000"));
  }
}
