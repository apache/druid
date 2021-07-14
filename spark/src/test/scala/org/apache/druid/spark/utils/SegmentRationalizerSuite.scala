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

package org.apache.druid.spark.utils

import org.apache.druid.java.util.common.Intervals
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.{LinearShardSpec, NumberedShardSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SegmentRationalizerSuite extends AnyFunSuite with Matchers {
  private val baseSegment: DataSegment = DataSegment.builder()
    .dataSource("test")
    .interval(Intervals.ETERNITY)
    .version("0")
    .shardSpec(new LinearShardSpec(0))
    .size(0)
    .build()

  test("rationalizeSegments should rationalize segments within time chunks, but not across them") {
    val nonRationalizedSegments = Seq[DataSegment](
      DataSegment.builder(baseSegment)
        .interval(Intervals.utc(0, 60000))
        .shardSpec(new LinearShardSpec(2))
        .size(1)
        .build(),
      DataSegment.builder(baseSegment)
        .interval(Intervals.utc(0, 60000))
        .shardSpec(new LinearShardSpec(5))
        .size(2)
        .build(),
      DataSegment.builder(baseSegment)
        .dataSource("quiz")
        .shardSpec(new NumberedShardSpec(4, 5))
        .size(3)
        .build(),
      DataSegment.builder(baseSegment)
        .shardSpec(new NumberedShardSpec(2, 6))
        .size(4)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(1, 3))
        .size(5)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(0, 3))
        .size(6)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(2, 3))
        .size(7)
        .build()
    )

    val expectedSegments = Seq[DataSegment](
      DataSegment.builder(baseSegment)
        .dataSource("quiz")
        .shardSpec(new NumberedShardSpec(0, 1))
        .size(3)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(0, 3))
        .size(6)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(1, 3))
        .size(5)
        .build(),
      DataSegment.builder(baseSegment)
        .version("1")
        .shardSpec(new NumberedShardSpec(2, 3))
        .size(7)
        .build(),
      DataSegment.builder(baseSegment)
        .shardSpec(new NumberedShardSpec(0, 1))
        .size(4)
        .build(),
      DataSegment.builder(baseSegment)
        .interval(Intervals.utc(0, 60000))
        .shardSpec(new LinearShardSpec(0))
        .size(1)
        .build(),
      DataSegment.builder(baseSegment)
        .interval(Intervals.utc(0, 60000))
        .shardSpec(new LinearShardSpec(1))
        .size(2)
        .build()
    )

    SegmentRationalizer.rationalizeSegments(nonRationalizedSegments) should(
      contain theSameElementsInOrderAs expectedSegments
      )
  }

  test("rationalizeGroupedSegments should return adjoining segments") {
    val nonRationalizedSegments = Seq[DataSegment](
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(1, 5)).size(1).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(4, 5)).size(2).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(4, 5)).size(3).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(7, 7)).size(4).build()
    )

    val expectedSegments = Seq[DataSegment](
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(0, 4)).size(1).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(1, 4)).size(2).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(2, 4)).size(3).build(),
      DataSegment.builder(baseSegment).shardSpec(new NumberedShardSpec(3, 4)).size(4).build()
    )

    SegmentRationalizer.rationalizeGroupedSegments(nonRationalizedSegments) should(
      contain theSameElementsInOrderAs expectedSegments
    )
  }
}
