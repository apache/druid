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

package org.apache.druid.data.input;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class MapBasedRowTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetLongMetricFromString()
  {
    MapBasedRow row = new MapBasedRow(
        DateTimes.nowUtc(),
        ImmutableMap.<String, Object>builder()
          .put("k0", "-1.2")
          .put("k1", "1.23")
          .put("k2", "1.8")
          .put("k3", "1e5")
          .put("k4", "9223372036854775806")
          .put("k5", "-9223372036854775807")
          .put("k6", "+9223372036854775802")
          .build()
    );
    
    Assert.assertEquals(-1.2, row.getMetric("k0"));
    Assert.assertEquals(1.23, row.getMetric("k1"));
    Assert.assertEquals(1.8, row.getMetric("k2"));
    Assert.assertEquals(100000.0, row.getMetric("k3"));
    Assert.assertEquals(9223372036854775806L, row.getMetric("k4"));
    Assert.assertEquals(-9223372036854775807L, row.getMetric("k5"));
    Assert.assertEquals(9223372036854775802L, row.getMetric("k6"));
  }

  @Test
  public void testImmutability()
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("k0", 1);
    event.put("k1", 2);
    final MapBasedRow row = new MapBasedRow(DateTimes.nowUtc(), event);
    expectedException.expect(UnsupportedOperationException.class);
    row.getEvent().put("k2", 3);
  }
}
