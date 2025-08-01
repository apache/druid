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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;

import java.util.function.Supplier;

/**
 * Additional utility methods used in embedded tests that are not added to
 * {@code Resources} to avoid cyclical dependencies.
 */
public class MoreResources
{
  /**
   * Task payload builders.
   */
  public static class Task
  {
    public static final Supplier<TaskBuilder.Index> BASIC_INDEX =
        () -> TaskBuilder
            .ofTypeIndex()
            .jsonInputFormat()
            .localInputSourceWithFiles(
                Resources.DataFile.TINY_WIKI_1_JSON,
                Resources.DataFile.TINY_WIKI_2_JSON,
                Resources.DataFile.TINY_WIKI_3_JSON
            )
            .timestampColumn("timestamp")
            .dimensions(
                "page",
                "language", "tags", "user", "unpatrolled", "newPage", "robot",
                "anonymous", "namespace", "continent", "country", "region", "city"
            )
            .metricAggregates(
                new CountAggregatorFactory("ingested_events"),
                new DoubleSumAggregatorFactory("added", "added"),
                new DoubleSumAggregatorFactory("deleted", "deleted"),
                new DoubleSumAggregatorFactory("delta", "delta"),
                new SketchMergeAggregatorFactory("thetaSketch", "user", null, null, null, null),
                new HllSketchBuildAggregatorFactory("HLLSketchBuild", "user", null, null, null, null, true),
                new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", null)
            )
            .dynamicPartitionWithMaxRows(3)
            .granularitySpec("DAY", "SECOND", true)
            .appendToExisting(false);
  }
}
