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

package org.apache.druid.spark.partitioners

import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.java.util.common.{DateTimes, IAE, ISE}
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.spark.MAPPER
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, HashPartitionFunction,
  ShardSpec, ShardSpecLookup}
import org.apache.spark.Partitioner

import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter,
  mapAsScalaMapConverter, seqAsJavaListConverter}

/**
  * Adapted from the Meta Markets druid-spark-batch project https://github.com/metamx/druid-spark-batch/, in particular
  * the DateAndHashBucketPartitioner and related utility functions.
  *
  * A Spark partitioner to partition rows in a DataFrame into partitions suitable for ingestion into Druid
  * using HashBasedNumberedShardSpecs. The important input piece is PARTMAP, a mapping from Druid time bucket
  * and segment partition id to the Spark input partition id that will handle. One way is to generate this mapping is
  * via the getSizedPartitionMap() function.
  *
  * @param granularityStr The Druid Granularity to partition by. This must match the granularity used to
  *                       generate PARTMAP
  * @param partMap A mapping from a tuple of Druid time bucket and Druid partition id to corresponding
  *                Spark partition id. See HashedNumberedSegmentPartitioner#getSizedPartitionMap
  * @param partitionDims The set of dimensions to hash using PARTITIONFUNCTION. If this is not specified,
  *                      all dimension plus the bucket time will be used
  * @param partitionFunction The function to use to hash PARTITIONDIMS. Defaults to MURMUR3_32_ABS.
  *                          See HashPartitionFunction for more details.
  */
class HashedNumberedSegmentPartitioner(
                             granularityStr: String,
                             partMap: Map[(Long, Long), Int],
                             partitionDims: Option[Set[String]] = None,
                             partitionFunction: HashPartitionFunction = HashPartitionFunction.MURMUR3_32_ABS
                           ) extends Partitioner with PartitionMapProvider {
  private lazy val granularity: Granularity = Granularity.fromString(granularityStr)
  private lazy val partitionDimensions: JList[String] = partitionDims.map(_.toList.asJava).orNull

  lazy val shardLookups: Map[Long, ShardSpecLookup] =
    partMap.groupBy { case ((bucket, druidId), sparkId) => bucket }
      .map { case (bucket, indices) => bucket -> indices.size }
      .mapValues(maxPartitions => {
        val shardSpecs = (0 until maxPartitions).map(
          partitionId => new HashBasedNumberedShardSpec(
            partitionId,
            maxPartitions,
            null, // scalastyle:ignore null
            null, // scalastyle:ignore null
            partitionDimensions,
            partitionFunction,
            MAPPER
          ).asInstanceOf[ShardSpec])
        shardSpecs.head.getLookup(shardSpecs.asJava)
      })

  override def numPartitions: Int = partMap.size

  override def getPartition(key: Any): Int = key match {
    case (k: Long, v: AnyRef) =>
      val eventMap: Map[String, AnyRef] = v match {
        case mm: JMap[String, AnyRef] @unchecked => mm.asScala.toMap
        case mm: Map[String, AnyRef] @unchecked => mm
        case x: Any => throw new IAE(s"Unknown value type ${x.getClass} : [$x]")
      }
      val dateBucket = granularity.bucketStart(DateTimes.utc(k)).getMillis
      val shardLookup = shardLookups.get(dateBucket) match {
        case Some(sl) => sl
        case None => throw new IAE(s"Bad date bucket $dateBucket")
      }

      val shardSpec = shardLookup.getShardSpec(
        dateBucket,
        new MapBasedInputRow(
          dateBucket,
          partitionDimensions,
          eventMap.asJava
        )
      )
      val timePartNum = shardSpec.getPartitionNum

      partMap.get((dateBucket.toLong, timePartNum)) match {
        case Some(part) => part
        case None => throw new ISE(s"bad date and partition combo: ($dateBucket, $timePartNum)")
      }
    case x: Any => throw new IAE(s"Unknown type ${x.getClass} : [$x]")
  }

  override def getPartitionMap: Map[Int, Map[String, String]] = {
    partMap.map{
      case ((bucket, druidPartitionId), sparkPartitionId) =>
        val properties = Map[String, String](
          "partitionId" -> druidPartitionId.toString,
          "numPartitions" -> numPartitions.toString,
          "bucketId" -> druidPartitionId.toString,
          "numBuckets" -> numPartitions.toString,
          "partitionDimensions" -> partitionDimensions.asScala.mkString(","),
          "hashPartitionFunction" -> partitionFunction.toString
        )
        sparkPartitionId -> properties
    }
  }
}

object HashedNumberedSegmentPartitioner {
  /**
    * Given a map of buckets to row counts for those buckets, return a map of
    * (bucket, druid partition id) -> spark partition id. Each spark partition id will have at most ROWSPERPARTITION
    * assuming random-ish hashing into the buckets. The input row count map BUCKETROWCOUNTMAP can be usually be
    * constructed from metadata if data is being ingested from other systems but will need to be generated in Spark
    * otherwise. If row counts per bucket aren't known, one possible approach is to use PartitionMapProvider#bucketDf
    * and PartitionMapProvider#getCountByBucket:
    *
    * val df = inputDf
    * val bucketedRdd = PartitionMapProvider.bucketDf(df, "tsCol", "tsFormat", "segmentGranularityString")
    * val partitioner = new HashedNumberedSegmentPartitioner(
    *   "segmentGranularityString",
    *   HashedNumberedSegmentPartitioner.getSizedPartitionMap(
    *     PartitionMapProvider.getCountByBucket(bucketedRdd), 5000000L)
    * )
    * sparkSession.createDataFrame(
    *   bucketedRdd.partitionBy(partitioner).map{case (_, map) => Row.fromSeq(df.schema.map(col => map(col.name)))},
    *   df.schema
    * )
    *
    * @param bucketRowCountMap A map of bucket to count of rows in that bucket
    * @param rowsPerPartition  The desired maximum rows per output partition.
    * @return A map of (bucket, druid partition id)->spark partition id . The size of this map times ROWSPERPARTITION
    *         is greater than or equal to the number of events (sum of values of bucketRowCountMap)
    */
  def getSizedPartitionMap(bucketRowCountMap: Map[Long, Long], rowsPerPartition: Long): Map[(Long, Long), Int] = {
    bucketRowCountMap
      .filter(_._2 > 0)
      .map(
        x => {
          val dateRangeBucket = x._1
          val numEventsInRange = x._2
          Range
            .apply(0, (numEventsInRange / rowsPerPartition + 1).toInt)
            .map(a => (dateRangeBucket, a.toLong))
        }
      )
      .foldLeft(Seq[(Long, Long)]())(_ ++ _)
      .foldLeft(Map[(Long, Long), Int]())(
        (b: Map[(Long, Long), Int], v: (Long, Long)) => b + (v -> b.size)
      )
  }
}
