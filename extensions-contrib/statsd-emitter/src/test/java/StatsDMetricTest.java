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

import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.emitter.statsd.StatsDMetric;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class StatsDMetricTest
{
  @Test
  public void testDefaultMetricDimensionsJsonDeSerialization() throws IOException
  {
    Map<String, StatsDMetric> metrics = StatsDEmitterTest.jsonMapper.readValue(
        this.getClass().getClassLoader().getResourceAsStream("defaultMetricDimensions.json"),
        new TypeReference<Map<String, StatsDMetric>>()
        {
        }
    );

    Assert.assertEquals(0.001, metrics.get("query/cpu/time").multiplier, 0);
    Assert.assertEquals(1, metrics.get("query/segmentAndCache/time").multiplier, 0);
  }

}
