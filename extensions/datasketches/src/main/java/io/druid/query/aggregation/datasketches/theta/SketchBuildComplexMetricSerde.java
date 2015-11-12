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

package io.druid.query.aggregation.datasketches.theta;

import com.metamx.common.IAE;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;
import io.druid.data.input.InputRow;
import io.druid.segment.serde.ComplexMetricExtractor;

import java.util.List;

/**
 */
public class SketchBuildComplexMetricSerde extends SketchMergeComplexMetricSerde
{
  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {

      @Override
      public Class<?> extractedClass()
      {
        return Object.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        Object obj = inputRow.getRaw(metricName);
        if (obj == null || obj instanceof Sketch || obj instanceof Memory) {
          return obj;
        }

        UpdateSketch sketch = Sketches.updateSketchBuilder().build(4096);
        if (obj instanceof String) {
          sketch.update((String) obj);
        } else if (obj instanceof byte[]) {
          sketch.update((byte[]) obj);
        } else if (obj instanceof Double) {
          sketch.update(((Double) obj));
        } else if (obj instanceof Integer || obj instanceof Long) {
          sketch.update(((Number) obj).longValue());
        } else if (obj instanceof int[]) {
          sketch.update((int[]) obj);
        } else if (obj instanceof long[]) {
          sketch.update((long[]) obj);
        } else if (obj instanceof List) {
          for (Object entry : (List) obj) {
            sketch.update(entry.toString());
          }
        } else {
          throw new IAE("Unknown object type[%s] received for ingestion.", obj.getClass());
        }

        return sketch;
      }
    };
  }
}
