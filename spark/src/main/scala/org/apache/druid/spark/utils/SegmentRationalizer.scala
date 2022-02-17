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

import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.ShardSpecRegistry
import org.apache.druid.timeline.DataSegment

/**
  * A utility class for rationalizing a group of Druid segments into contiguous atomically-swappable segments.
  * This class does not change the underlying segments, and in particular does not change their names or
  * locations on deep storage, only the metadata saved into the Druid cluster.
  */
object SegmentRationalizer extends Logging {
  /**
    * Given a list of DataSegments SEGMENTS, return a possibly modified list of DataSegments that for each datasource,
    * interval, and version contains segments matching the source segments with their partition numbers and number of
    * core partitions modified to be sequential and complete, e.g. for a time chunk defined by a data source, interval,
    * and version all segments produced by this method will contain linearly increasing partition numbers from 0 to
    * (# of partitions in the chunk) - 1.
    *
    * @param segments A list of DataSegments.
    * @return A modified list of DataSegments containing the same ShardSpec-specific properties but with their partition
    *         number and number of core partition properties updated to follow a linaerly increasing progression from 0
    *         to # of core partitions - 1 for each time chunk in the list.
    */
  def rationalizeSegments(segments: Seq[DataSegment]): Seq[DataSegment] = {
    segments
      .groupBy(segment => (segment.getDataSource, segment.getInterval)).flatMap{
      case ((datasource, interval), intervalSegments) =>
        if (intervalSegments.map(_.getShardSpec.getClass).distinct.size != 1) {
          logWarn("This writer does not know how to rationalize multiple shard spec types for a single interval!")
          intervalSegments
        } else {
          val versions = intervalSegments.map(_.getVersion)
          if (versions.distinct.size != 1) {
            // Rather than aborting all segments or passing back a list of segments to commit and a list of
            // segments to shadow abort which is way more dangerous than is really safe to handle in the
            // committer, just warn the user that some segments will be overshadowed and ignored.
            logWarn(
              s"More than one version detected for interval ${interval.toString} on dataSource $datasource! " +
                s"Some segments will be overshadowed! Versions detected: ${versions.mkString(", ")}")
            intervalSegments.groupBy(_.getVersion).flatMap(segments => rationalizeGroupedSegments(segments._2))
          } else {
            rationalizeGroupedSegments(intervalSegments)
          }
        }
    }.toSeq
  }

  /**
    * Given a list of DataSegments SEGMENTS pertaining to a single time chunk, rationalizes their ShardSpecs to be
    * contiguous and complete.
    *
    * @param segments A list of DataSegments for a single time chunk.
    * @return SEGMENTS, modified to contain contiguous and complete ShardSpecs.
    */
  private[utils] def rationalizeGroupedSegments(segments: Seq[DataSegment]): Seq[DataSegment] = {
    val numPartitions = segments.size
    segments.sortBy(_.getShardSpec.getPartitionNum).zipWithIndex.map{
      case (segment, index) =>
        segment.withShardSpec(ShardSpecRegistry.updateShardSpec(segment.getShardSpec, index, numPartitions))
    }
  }
}
