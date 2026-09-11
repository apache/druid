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
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class KafkaHeaderBasedFilterConfigTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @BeforeAll
  public static void setUpStatic()
  {
    ExpressionProcessing.initializeForTests();
  }

  @Test
  public void testInFilterSingleValue()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig filter = new KafkaHeaderBasedFilterConfig(dimFilter, null, null);

    Assertions.assertEquals(dimFilter, filter.getFilter());
    Assertions.assertEquals("UTF-8", filter.getEncoding());
    Assertions.assertEquals(10_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testInFilterMultipleValues()
  {
    InDimFilter dimFilter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilterConfig filter = new KafkaHeaderBasedFilterConfig(dimFilter, "ISO-8859-1", null);

    Assertions.assertEquals(dimFilter, filter.getFilter());
    Assertions.assertEquals("ISO-8859-1", filter.getEncoding());
    Assertions.assertEquals(10_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testInFilterWithCustomCacheSize()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig filter = new KafkaHeaderBasedFilterConfig(dimFilter, null, 50_000);

    Assertions.assertEquals(dimFilter, filter.getFilter());
    Assertions.assertEquals("UTF-8", filter.getEncoding());
    Assertions.assertEquals(50_000, filter.getStringDecodingCacheSize());
  }

  @Test
  public void testSelectorFilterRejected()
  {
    SelectorDimFilter dimFilter = new SelectorDimFilter("environment", "production", null);
    try {
      new KafkaHeaderBasedFilterConfig(dimFilter, null, null);
      Assertions.fail("Expected DruidException for SelectorDimFilter");
    }
    catch (DruidException e) {
      Assertions.assertTrue(e.getMessage().contains("Unsupported filter type"), "Should mention unsupported filter type");
      Assertions.assertTrue(e.getMessage().contains("SelectorDimFilter"), "Should mention SelectorDimFilter");
    }
  }

  @Test
  public void testAndFilterRejected()
  {
    SelectorDimFilter envFilter = new SelectorDimFilter("environment", "production", null);
    SelectorDimFilter serviceFilter = new SelectorDimFilter("service", "user-service", null);
    AndDimFilter andFilter = new AndDimFilter(Arrays.asList(envFilter, serviceFilter));
    try {
      new KafkaHeaderBasedFilterConfig(andFilter, null, null);
      Assertions.fail("Expected DruidException for AndDimFilter");
    }
    catch (DruidException e) {
      Assertions.assertTrue(e.getMessage().contains("Unsupported filter type"), "Should mention unsupported filter type");
      Assertions.assertTrue(e.getMessage().contains("AndDimFilter"), "Should mention AndDimFilter");
    }
  }

  @Test
  public void testNotFilterRejected()
  {
    SelectorDimFilter debugFilter = new SelectorDimFilter("debug-mode", "true", null);
    NotDimFilter notFilter = new NotDimFilter(debugFilter);
    try {
      new KafkaHeaderBasedFilterConfig(notFilter, null, null);
      Assertions.fail("Expected DruidException for NotDimFilter");
    }
    catch (DruidException e) {
      Assertions.assertTrue(e.getMessage().contains("Unsupported filter type"), "Should mention unsupported filter type");
      Assertions.assertTrue(e.getMessage().contains("NotDimFilter"), "Should mention NotDimFilter");
    }
  }

  @Test
  public void testInFilterWithExtractionFnRejected()
  {
    // An 'in' filter with an extractionFn is a valid Druid filter, but the header handler ignores the function.
    // It must be rejected rather than silently evaluated with different semantics.
    InDimFilter filter = new InDimFilter(
        "environment",
        Collections.singletonList("production"),
        new SubstringDimExtractionFn(0, 5)
    );
    try {
      new KafkaHeaderBasedFilterConfig(filter, null, null);
      Assertions.fail("Expected DruidException for InDimFilter with extractionFn");
    }
    catch (DruidException e) {
      Assertions.assertTrue(e.getMessage().contains("Extraction functions"), "Should mention extraction functions");
    }
  }

  @Test
  public void testNullFilter()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new KafkaHeaderBasedFilterConfig(null, null, null)
    );
  }

  @Test
  public void testInvalidEncoding()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KafkaHeaderBasedFilterConfig(dimFilter, "INVALID-ENCODING", null)
    );
  }

  @Test
  public void testSerialization() throws Exception
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig originalFilter = new KafkaHeaderBasedFilterConfig(dimFilter, "UTF-16", null);

    // Serialize to JSON
    String json = objectMapper.writeValueAsString(originalFilter);

    // Deserialize back
    KafkaHeaderBasedFilterConfig deserializedFilter = objectMapper.readValue(json, KafkaHeaderBasedFilterConfig.class);

    Assertions.assertEquals(originalFilter.getFilter(), deserializedFilter.getFilter());
    Assertions.assertEquals(originalFilter.getEncoding(), deserializedFilter.getEncoding());
    Assertions.assertEquals(originalFilter.getStringDecodingCacheSize(), deserializedFilter.getStringDecodingCacheSize());
  }

  @Test
  public void testEquals()
  {
    InDimFilter dimFilter1 = new InDimFilter("environment", Collections.singletonList("production"), null);
    InDimFilter dimFilter2 = new InDimFilter("environment", Collections.singletonList("production"), null);
    InDimFilter dimFilter3 = new InDimFilter("environment", Collections.singletonList("staging"), null);

    KafkaHeaderBasedFilterConfig filter1 = new KafkaHeaderBasedFilterConfig(dimFilter1, "UTF-8", null);
    KafkaHeaderBasedFilterConfig filter2 = new KafkaHeaderBasedFilterConfig(dimFilter2, "UTF-8", null);
    KafkaHeaderBasedFilterConfig filter3 = new KafkaHeaderBasedFilterConfig(dimFilter3, "UTF-8", null);
    KafkaHeaderBasedFilterConfig filter4 = new KafkaHeaderBasedFilterConfig(dimFilter1, "UTF-16", null);
    KafkaHeaderBasedFilterConfig filter5 = new KafkaHeaderBasedFilterConfig(dimFilter1, "UTF-8", 5000);

    Assertions.assertEquals(filter1, filter2);
    Assertions.assertNotEquals(filter1, filter3);
    Assertions.assertNotEquals(filter1, filter4);
    Assertions.assertNotEquals(filter1, filter5); // Different cache size
    Assertions.assertNotEquals(filter1, null);
    Assertions.assertNotEquals(filter1, "string");
  }

  @Test
  public void testToString()
  {
    InDimFilter dimFilter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilterConfig filter = new KafkaHeaderBasedFilterConfig(dimFilter, "UTF-8", null);

    String toString = filter.toString();
    Assertions.assertTrue(toString.contains("KafkaheaderBasedFilterConfig"));
    Assertions.assertTrue(toString.contains("filter="));
    Assertions.assertTrue(toString.contains("encoding='UTF-8'"));
    Assertions.assertTrue(toString.contains("stringDecodingCacheSize=10000"));
  }
}
