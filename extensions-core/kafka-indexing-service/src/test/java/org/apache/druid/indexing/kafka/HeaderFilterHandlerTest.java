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

package org.apache.druid.indexing.kafka;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HeaderFilterHandlerTest
{
  @Test
  public void testInDimFilterHandler()
  {
    // Create an InDimFilter for testing
    InDimFilter filter = new InDimFilter("environment", ImmutableSet.of("production", "staging"));

    // Create handler using our extensible factory
    HeaderFilterHandler handler = HeaderFilterHandlerFactory.forFilter(filter);

    // Verify it's the correct type
    Assertions.assertTrue(handler instanceof InDimFilterHandler, "Handler should be InDimFilterHandler");

    // Test header name extraction
    Assertions.assertEquals("environment", handler.getHeaderName());

    // Test matching values
    Assertions.assertTrue(handler.shouldInclude("production"), "Production should be included");
    Assertions.assertTrue(handler.shouldInclude("staging"), "Staging should be included");

    // Test non-matching values
    Assertions.assertFalse(handler.shouldInclude("development"), "Development should be excluded");
    Assertions.assertFalse(handler.shouldInclude("test"), "Test should be excluded");

    // Test description
    String description = handler.getDescription();
    Assertions.assertTrue(description.contains("InDimFilter"), "Description should contain filter type");
    Assertions.assertTrue(description.contains("environment"), "Description should contain header name");
    Assertions.assertTrue(description.contains("2"), "Description should contain value count");
  }

  @Test
  public void testUnsupportedFilterType()
  {
    // Create a mock filter that's not supported
    Filter unsupportedFilter = TrueDimFilter.instance().toFilter();

    // Should throw IllegalArgumentException
    try {
      HeaderFilterHandlerFactory.forFilter(unsupportedFilter);
      Assertions.fail("Should have thrown IllegalArgumentException for unsupported filter type");
    }
    catch (IllegalArgumentException e) {
      Assertions.assertTrue(
                       e.getMessage().contains("Unsupported filter type"), "Error message should mention unsupported type");
      Assertions.assertTrue(
                       e.getMessage().contains("True"), "Error message should mention True");
    }
  }
}
