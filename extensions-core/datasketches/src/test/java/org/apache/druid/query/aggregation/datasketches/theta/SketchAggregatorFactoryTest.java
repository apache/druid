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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.oldapi.OldSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.oldapi.OldSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class SketchAggregatorFactoryTest
{
  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new OldSketchBuildAggregatorFactory("oldBuild", "col", 16),
                  new OldSketchMergeAggregatorFactory("oldMerge", "col", 16, false),
                  new OldSketchMergeAggregatorFactory("oldMergeFinalize", "col", 16, true),
                  new SketchMergeAggregatorFactory("merge", "col", 16, false, false, null),
                  new SketchMergeAggregatorFactory("mergeFinalize", "col", 16, true, false, null)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("oldBuild-access", "oldBuild"),
                  new FinalizingFieldAccessPostAggregator("oldBuild-finalize", "oldBuild"),
                  new FieldAccessPostAggregator("oldMerge-access", "oldMerge"),
                  new FinalizingFieldAccessPostAggregator("oldMerge-finalize", "oldMerge"),
                  new FieldAccessPostAggregator("oldMergeFinalize-access", "oldMergeFinalize"),
                  new FinalizingFieldAccessPostAggregator("oldMergeFinalize-finalize", "oldMergeFinalize"),
                  new FieldAccessPostAggregator("merge-access", "merge"),
                  new FinalizingFieldAccessPostAggregator("merge-finalize", "merge"),
                  new FieldAccessPostAggregator("mergeFinalize-access", "mergeFinalize"),
                  new FinalizingFieldAccessPostAggregator("mergeFinalize-finalize", "mergeFinalize"),
                  new SketchEstimatePostAggregator(
                      "sketchEstimate",
                      new FieldAccessPostAggregator(null, "merge"),
                      null
                  ),
                  new SketchEstimatePostAggregator(
                      "sketchEstimateStdDev",
                      new FieldAccessPostAggregator(null, "merge"),
                      2
                  ),
                  new SketchSetPostAggregator(
                      "sketchSet",
                      "UNION",
                      null,
                      ImmutableList.of(
                          new FieldAccessPostAggregator(null, "oldMerge"),
                          new FieldAccessPostAggregator(null, "merge")
                      )
                  ),
                  new SketchToStringPostAggregator(
                      "sketchString",
                      new FieldAccessPostAggregator(null, "merge")
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ValueType.LONG)
                    .add("oldBuild", null)
                    .add("oldMerge", ValueType.COMPLEX)
                    .add("oldMergeFinalize", null)
                    .add("merge", ValueType.COMPLEX)
                    .add("mergeFinalize", null)
                    .add("oldBuild-access", ValueType.COMPLEX)
                    .add("oldBuild-finalize", ValueType.DOUBLE)
                    .add("oldMerge-access", ValueType.COMPLEX)
                    .add("oldMerge-finalize", ValueType.COMPLEX)
                    .add("oldMergeFinalize-access", ValueType.COMPLEX)
                    .add("oldMergeFinalize-finalize", ValueType.DOUBLE)
                    .add("merge-access", ValueType.COMPLEX)
                    .add("merge-finalize", ValueType.COMPLEX)
                    .add("mergeFinalize-access", ValueType.COMPLEX)
                    .add("mergeFinalize-finalize", ValueType.DOUBLE)
                    .add("sketchEstimate", ValueType.DOUBLE)
                    .add("sketchEstimateStdDev", ValueType.COMPLEX)
                    .add("sketchSet", ValueType.COMPLEX)
                    .add("sketchString", ValueType.STRING)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
