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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Performs Student's t-test and returns a list of p-values given two instances of {@link ArrayOfDoublesSketch}.
 * The result will be N double values, where N is the number of double values kept in the sketch per key.
 * See <a href=http://commons.apache.org/proper/commons-math/javadocs/api-3.4/org/apache/commons/math3/stat/inference/TTest.html>Student's t-test</a>
 */
public class ArrayOfDoublesSketchTTestPostAggregator extends ArrayOfDoublesSketchMultiPostAggregator
{

  @JsonCreator
  public ArrayOfDoublesSketchTTestPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    super(name, fields);
    if (fields.size() != 2) {
      throw new IAE("Illegal number of fields[%d], must be 2", fields.size());
    }
  }

  @Override
  public Comparator<double[]> getComparator()
  {
    throw new IAE("Comparing arrays of p values is not supported");
  }

  @Override
  public double[] compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch1 = (ArrayOfDoublesSketch) getFields().get(0).compute(combinedAggregators);
    final ArrayOfDoublesSketch sketch2 = (ArrayOfDoublesSketch) getFields().get(1).compute(combinedAggregators);
    if (sketch1.getNumValues() != sketch2.getNumValues()) {
      throw new IAE(
          "Sketches have different number of values: %d and %d",
          sketch1.getNumValues(),
          sketch2.getNumValues()
      );
    }

    final SummaryStatistics[] stats1 = getStats(sketch1);
    final SummaryStatistics[] stats2 = getStats(sketch2);

    final int numberOfValues = sketch1.getNumValues();
    final double[] pValues = new double[numberOfValues];
    final TTest test = new TTest();
    for (int i = 0; i < pValues.length; i++) {
      pValues[i] = test.tTest(stats1[i], stats2[i]);
    }
    return pValues;
  }

  private static SummaryStatistics[] getStats(final ArrayOfDoublesSketch sketch)
  {
    final SummaryStatistics[] stats = new SummaryStatistics[sketch.getNumValues()];
    Arrays.setAll(stats, i -> new SummaryStatistics());
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      final double[] values = it.getValues();
      for (int i = 0; i < values.length; i++) {
        stats[i].addValue(values[i]);
      }
    }
    return stats;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_T_TEST_CACHE_TYPE_ID)
        .appendCacheables(getFields())
        .build();
  }

}
