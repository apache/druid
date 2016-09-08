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

package io.druid.data.input;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class MapBasedRowTest
{
  @Test
  public void testGetLongMetricFromString()
  {
    MapBasedRow row = new MapBasedRow(
        new DateTime(),
        ImmutableMap.<String,Object>builder()
          .put("k0", "-1.2")
          .put("k1", "1.23")
          .put("k2", "1.8")
          .put("k3", "1e5")
          .put("k4", "9223372036854775806")
          .put("k5", "-9223372036854775807")
          .put("k6", "+9223372036854775802")
          .build()
    );
    
    Assert.assertEquals(-1, row.getLongMetric("k0"));
    Assert.assertEquals(1, row.getLongMetric("k1"));
    Assert.assertEquals(1, row.getLongMetric("k2"));
    Assert.assertEquals(100000, row.getLongMetric("k3"));
    Assert.assertEquals(9223372036854775806L, row.getLongMetric("k4"));
    Assert.assertEquals(-9223372036854775807L, row.getLongMetric("k5"));
    Assert.assertEquals(9223372036854775802L, row.getLongMetric("k6"));
  }
}
