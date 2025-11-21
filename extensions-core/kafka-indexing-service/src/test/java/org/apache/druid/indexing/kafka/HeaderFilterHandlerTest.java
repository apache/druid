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
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertTrue("Handler should be InDimFilterHandler", handler instanceof InDimFilterHandler);

    // Test header name extraction
    Assert.assertEquals("environment", handler.getHeaderName());

    // Test matching values
    Assert.assertTrue("Production should be included", handler.shouldInclude("production"));
    Assert.assertTrue("Staging should be included", handler.shouldInclude("staging"));

    // Test non-matching values
    Assert.assertFalse("Development should be excluded", handler.shouldInclude("development"));
    Assert.assertFalse("Test should be excluded", handler.shouldInclude("test"));

    // Test description
    String description = handler.getDescription();
    Assert.assertTrue("Description should contain filter type", description.contains("InDimFilter"));
    Assert.assertTrue("Description should contain header name", description.contains("environment"));
    Assert.assertTrue("Description should contain value count", description.contains("2"));
  }

  @Test
  public void testUnsupportedFilterType()
  {
    // Create a mock filter that's not supported
    Filter unsupportedFilter = TrueDimFilter.instance().toFilter();

    // Should throw IllegalArgumentException
    try {
      HeaderFilterHandlerFactory.forFilter(unsupportedFilter);
      Assert.fail("Should have thrown IllegalArgumentException for unsupported filter type");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("Error message should mention unsupported type",
                       e.getMessage().contains("Unsupported filter type"));
      Assert.assertTrue("Error message should mention True",
                       e.getMessage().contains("True"));
    }
  }
}
