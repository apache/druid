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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 * Test utility to extract and verify metric values.
 */
public interface MetricsVerifier
{
  /**
   * Verifies that no event has been emitted for the given metric.
   */
  default void verifyNotEmitted(String metricName)
  {
    verifyEmitted(metricName, 0);
  }

  /**
   * Verifies that the metric was emitted the expected number of times.
   */
  default void verifyEmitted(String metricName, int times)
  {
    verifyEmitted(metricName, null, times);
  }

  /**
   * Verifies that the metric was emitted for the given dimension filters the
   * expected number of times.
   */
  default void verifyEmitted(String metricName, Map<String, Object> dimensionFilters, int times)
  {
    Assert.assertEquals(
        StringUtils.format("Metric [%s] was emitted unexpected number of times.", metricName),
        times,
        getMetricValues(metricName, dimensionFilters).size()
    );
  }

  /**
   * Verifies the value of the specified metric emitted in the previous run.
   */
  default void verifyValue(String metricName, Number expectedValue)
  {
    verifyValue(metricName, null, expectedValue);
  }

  /**
   * Verifies the value of the event corresponding to the specified metric and
   * dimensionFilters emitted in the previous run.
   */
  default void verifyValue(String metricName, Map<String, Object> dimensionFilters, Number expectedValue)
  {
    Assert.assertEquals(expectedValue, getValue(metricName, dimensionFilters));
  }

  /**
   * Gets the value of the event corresponding to the specified metric and
   * dimensionFilters.
   */
  default Number getValue(String metricName, Map<String, Object> dimensionFilters)
  {
    List<Number> values = getMetricValues(metricName, dimensionFilters);
    Assert.assertEquals(
        "Metric must have been emitted exactly once for the given dimensions.",
        1,
        values.size()
    );
    return values.get(0);
  }

  /**
   * Gets the metric values for the specified dimension filters.
   */
  List<Number> getMetricValues(String metricName, Map<String, Object> dimensionFilters);

}
