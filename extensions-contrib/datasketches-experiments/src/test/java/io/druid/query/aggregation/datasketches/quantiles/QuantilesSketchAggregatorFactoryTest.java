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

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class QuantilesSketchAggregatorFactoryTest
{
  private final ObjectMapper mapper;

  public QuantilesSketchAggregatorFactoryTest()
  {
    mapper = new DefaultObjectMapper();
    QuantilesSketchModule sm = new QuantilesSketchModule();
    for(Module mod : sm.getJacksonModules()) {
      mapper.registerModule(mod);
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    assertAggregatorFactorySerde(
        makeJson("name", "fieldName", 16, null, null),
        new QuantilesSketchAggregatorFactory("name", "fieldName", 16, false, null)
    );

    assertAggregatorFactorySerde(
        makeJson("name", "fieldName", 16, true, 1024),
        new QuantilesSketchAggregatorFactory("name", "fieldName", 16, true, 1024)
    );

    assertAggregatorFactorySerde(
        makeJson("name", "fieldName", 16, false, 4096),
        new QuantilesSketchAggregatorFactory("name", "fieldName", 16, false, 4096)
    );
  }

  @Test
  public void testFinalization() throws Exception
  {
    DoublesSketch sketch = new DoublesSketchBuilder().build();

    QuantilesSketchAggregatorFactory agg = new QuantilesSketchAggregatorFactory("name", "fieldName", 16, false, null);
    Assert.assertEquals(0L, agg.finalizeComputation(sketch));
  }

  private String makeJson(
      String name,
      String fieldName,
      Integer size,
      Boolean isInputSketch,
      Integer maxIntermediateSize
  )
  {
    String str = "{\n"
                 + "  \"name\": \"" + name + "\",\n"
                 + "  \"fieldName\": \"" + fieldName + "\",\n";

    if (size != null) {
      str += "  \"size\": " + size + ",\n";
    }

    if (isInputSketch != null) {
      str += "  \"isInputSketch\": " + isInputSketch + ",\n";
    }

    if (maxIntermediateSize != null) {
      str += "  \"maxIntermediateSize\": " + maxIntermediateSize + ",\n";
    }

    str += "  \"type\": \"datasketchesQuantilesSketch\"\n }";

    return str;
  }

  private void assertAggregatorFactorySerde(String aggJson, AggregatorFactory expected) throws Exception
  {
    Assert.assertEquals(
        expected,
        mapper.readValue(aggJson, AggregatorFactory.class)
    );

//    Assert.assertEquals(
//        expected,
//        mapper.readValue(
//            mapper.writeValueAsString(
//                mapper.readValue(aggJson, AggregatorFactory.class)
//            ),
//            AggregatorFactory.class
//        )
//    );
  }

  public static final ObjectMapper buildObjectMapper()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    QuantilesSketchModule sm = new QuantilesSketchModule();
    for(Module mod : sm.getJacksonModules()) {
      mapper.registerModule(mod);
    }
    return mapper;
  }
}
