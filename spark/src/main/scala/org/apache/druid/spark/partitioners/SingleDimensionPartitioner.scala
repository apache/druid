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

import org.apache.druid.java.util.common.ISE
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, dense_rank, lit, rank}
import org.apache.spark.sql.types.IntegerType

class SingleDimensionPartitioner(df: DataFrame) extends PartitionMapProvider with Serializable {
  private var partitionMap = Map[Int, Map[String, String]]()

  def partition(
                 tsCol: String,
                 tsFormat: String,
                 segmentGranularity: String,
                 rowsPerPartition: Long,
                 partitionCol: String,
                 shouldRollUp: Boolean
               ): DataFrame = {
    // We use rank() here even if rollup is false so that every row with the same value for partitionCol
    // will end up in the same Druid partition. This does mean we may overshoot rowsPerPartition.
    val rankColFunc = if (shouldRollUp) dense_rank() else rank()
    val bucketedDf = df
      .withColumn(_timeBucketCol, SparkUdfs.bucketRow(col(tsCol), lit(tsFormat), lit(segmentGranularity)))
      .withColumn(_rankCol, rankColFunc.over(timeBucketWindowSpec(partitionCol)))
      .withColumn(_partitionNumCol,
        col(_rankCol)
          .divide(lit(rowsPerPartition))
          .cast(IntegerType))

    val partitionedDf = bucketedDf.repartition(col(_timeBucketCol), col(_partitionNumCol))
    partitionMap = SingleDimensionPartitioner.generatePartitionMap(partitionedDf, partitionCol)
    partitionedDf.drop(_timeBucketCol, _rankCol, _partitionNumCol)
  }

  override def getPartitionMap: Map[Int, Map[String, String]] = {
    if (partitionMap.isEmpty) {
      throw new ISE("Must call partition() to partition the dataframe before calling getPartitionMap!")
    }
    partitionMap
  }
}

object SingleDimensionPartitioner {
  private[spark] def generatePartitionMap(
                                           partitionedDf: DataFrame,
                                           partitionCol: String
                                         ): Map[Int, Map[String, String]] = {
    val bucketMappings = partitionedDf.rdd.mapPartitions{rowIterator =>
      if (rowIterator.hasNext) {
        val head = rowIterator.take(1).next()
        val sparkPartitionId = TaskContext.getPartitionId()
        val druidPartitionId = head.getInt(head.fieldIndex(_partitionNumCol))
        val timeBucket = head.getLong(head.fieldIndex(_timeBucketCol))
        val start = head.get(head.fieldIndex(partitionCol))
        Iterator(SingleDimensionPartitionInfo(timeBucket, druidPartitionId, sparkPartitionId, start))
      } else {
        Iterator.empty
      }
    }.collect().groupBy(_.timeBucket)

    bucketMappings.values.flatMap{partitions =>
      val numPopulatedPartitions = partitions.length
      val orderedPartitions = partitions.sortBy(_.druidPartition)
      orderedPartitions.zipWithIndex.map{ case(partition, index) =>
        // If we're the first partition, we have an open lower bound. Likewise, if we're the last partition, we have
        // an open upper bound. Otherwise, take start as our start and the start from the next partition as our end.
        val boundsMap = Seq(
          if (partition.druidPartition != 0) Some("start" -> partition.start.toString) else None,
          if (index != (numPopulatedPartitions - 1)) {
            Some("end" -> orderedPartitions(index + 1).start.toString)
          } else {
            None
          }
        ).flatten.toMap

        partition.sparkPartition -> (Map[String, String](
          "partitionId" -> index.toString,
          "dimension" -> partitionCol,
          "numPartitions" -> numPopulatedPartitions.toString
        ) ++ boundsMap)
      }
    }.toMap
  }

  private case class SingleDimensionPartitionInfo(
                                                   timeBucket: Long,
                                                   druidPartition: Int,
                                                   sparkPartition: Int,
                                                   start: Any
                                                 )

}
