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

import org.apache.druid.java.util.common.{FileUtils, ISE, StringUtils}
import org.apache.druid.segment.{IndexIO, QueryableIndexSegment, Segment, SegmentLazyLoadFailCallback}
import org.apache.druid.segment.loading.SegmentLoader
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.SegmentReaderRegistry
import org.apache.druid.timeline.DataSegment
import org.apache.hadoop.conf.{Configuration => HConf}

import java.io.File
import scala.collection.mutable.{HashSet => MHashSet}

/**
  * A SegmentLoader to manage loading segment files for a partition reader. For now, a segment loader is created per
  * PartitionReader, which means it will only ever load a single segment. This is slightly wasteful, but avoids needing
  * to manage the SegmentLoader's life cycle outside of a PartitionReader. If the input partition planning logic ever
  * gets smarter than just assigning each segment to a partition, this design decision should be revisited.
  */
class PartitionReaderSegmentLoader(
                                  val tmpDir: File,
                                  val hadoopConf: HConf,
                                  val indexIO: IndexIO
                                  ) extends SegmentLoader with Logging {
  private val loadedSegments = new MHashSet[DataSegment]

  override def isSegmentLoaded(segment: DataSegment): Boolean = loadedSegments.contains(segment)

  override def getSegment(segment: DataSegment, `lazy`: Boolean, loadFailed: SegmentLazyLoadFailCallback): Segment = {
    val segmentFile = getSegmentFiles(segment)
    val index = indexIO.loadIndex(segmentFile, `lazy`, loadFailed)
    logInfo(s"Loaded segment [${segment.getId}].")
    new QueryableIndexSegment(index, segment.getId)
  }

  override def getSegmentFiles(segment: DataSegment): File = {
    val segmentDir = new File(tmpDir, segment.getId.toString)
    if (!segmentDir.exists) {
      logInfo(
        StringUtils.format(
          "Fetching segment [%s] to [%s].", segment.getId, segmentDir
        )
      )
      if (!segmentDir.mkdir) throw new ISE("Failed to make directory[%s]", segmentDir)
      SegmentReaderRegistry.load(segment.getLoadSpec, segmentDir, hadoopConf)
      loadedSegments += segment
    }
    segmentDir
  }

  override def cleanup(segment: DataSegment): Unit = {
    if (isSegmentLoaded(segment)) {
      loadedSegments -= segment
      FileUtils.deleteDirectory(new File(tmpDir, segment.getId.toString))
    }
  }
}
