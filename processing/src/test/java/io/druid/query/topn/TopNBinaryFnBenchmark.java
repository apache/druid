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

package io.druid.query.topn;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopNBinaryFnBenchmark extends SimpleBenchmark
{
  @Param({"1", "5", "10", "15"})
  int aggCount;
  @Param({"1", "5", "10", "15"})
  int postAggCount;
  @Param({"1000", "10000"})
  int threshold;
  Result<TopNResultValue> result1;
  Result<TopNResultValue> result2;
  TopNBinaryFn fn;

  public static void main(String[] args) throws Exception
  {
    Runner.main(TopNBinaryFnBenchmark.class, args);
  }

  @Override
  protected void setUp() throws Exception
  {

    final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
    final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
    final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");


    final List<AggregatorFactory> aggregatorFactories = new ArrayList<>();
    aggregatorFactories.add(new CountAggregatorFactory("rows"));
    aggregatorFactories.add(new LongSumAggregatorFactory("index", "index"));
    for (int i = 1; i < aggCount; i++) {
      aggregatorFactories.add(new CountAggregatorFactory("rows" + i));
    }
    final List<PostAggregator> postAggregators = new ArrayList<>();
    for (int i = 0; i < postAggCount; i++) {
      postAggregators.add(
          new ArithmeticPostAggregator(
              "addrowsindexconstant" + i,
              "+",
              Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
          )
      );
    }
    final DateTime currTime = new DateTime();
    List<Map<String, Object>> list = new ArrayList<>();
    for (int i = 0; i < threshold; i++) {
      Map<String, Object> res = new HashMap<>();
      res.put("testdim", "" + i);
      res.put("rows", 1L);
      for (int j = 0; j < aggCount; j++) {
        res.put("rows" + j, 1L);
      }
      res.put("index", 1L);
      list.add(res);
    }
    result1 = new Result<>(
        currTime,
        new TopNResultValue(list)
    );

    List<Map<String, Object>> list2 = new ArrayList<>();
    for (int i = 0; i < threshold; i++) {
      Map<String, Object> res = new HashMap<>();
      res.put("testdim", "" + i);
      res.put("rows", 2L);
      for (int j = 0; j < aggCount; j++) {
        res.put("rows" + j, 2L);
      }
      res.put("index", 2L);
      list2.add(res);
    }
    result2 = new Result<>(
        currTime,
        new TopNResultValue(list2)
    );
    fn = new TopNBinaryFn(
        Granularities.ALL,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("index"),
        100,
        aggregatorFactories,
        postAggregators
    );
  }

  public void timeMerge(int nReps)
  {
    for (int i = 0; i < nReps; i++) {
      fn.apply(result1, result2);
    }
  }

}
