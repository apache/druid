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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, dense_rank, lit, max, row_number}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class NumberedPartitioner(df: DataFrame) extends PartitionMapProvider with Serializable {
  private var partitionMap = Map[Int, Map[String, String]]()

  def partition(
                 tsCol: String,
                 tsFormat: String,
                 segmentGranularity: String,
                 rowsPerPartition: Long,
                 shouldRollUp: Boolean,
                 dimensions: Option[Seq[String]] = None
               ): DataFrame = {
    val dimensionCols = dimensions.map(_.map(col))
      .getOrElse(df.schema.fieldNames.toSeq.filterNot(_ == tsCol).map(col))

    val windowSpec =
      Window.partitionBy(_timeBucketCol).orderBy(dimensionCols:_*)

    val rankColFunc = if (shouldRollUp) dense_rank() else row_number()
    val bucketedDf = df
      .withColumn(_timeBucketCol, SparkUdfs.bucketRow(col(tsCol), lit(tsFormat), lit(segmentGranularity)))
      .withColumn(_rankCol, rankColFunc.over(windowSpec))
      .withColumn(_partitionNumCol,
        max(_rankCol)
          .over(timeBucketWindowSpec(_timeBucketCol)
            .rangeBetween(Window.currentRow, Window.unboundedFollowing))
          .divide(lit(rowsPerPartition))
          .cast(IntegerType))

    val partitionedDf = bucketedDf
      .repartition(col(_timeBucketCol), ceil(col(_rankCol).cast(DoubleType).divide(lit(rowsPerPartition))))
    partitionMap = NumberedPartitioner.generatePartitionMap(partitionedDf, rowsPerPartition)
    partitionedDf.drop(_timeBucketCol, _rankCol, _partitionNumCol)
  }

  override def getPartitionMap: Map[Int, Map[String, String]] = {
    if (partitionMap.isEmpty) {
      throw new ISE("Must call partition() to partition the dataframe before calling getPartitionMap!")
    }
    partitionMap
  }
}

object NumberedPartitioner {
  private[spark] def generatePartitionMap(
                                           partitionedDf: DataFrame,
                                           rowsPerPartition: Long
                                         ): Map[Int, Map[String, String]] = {

    partitionedDf.rdd.mapPartitions{rowIterator =>
      if (rowIterator.hasNext) {
        val sparkPartitionId = TaskContext.getPartitionId()
        val head = rowIterator.next()
        Iterator(sparkPartitionId -> Map[String, String](
          "partitionId" -> (head.getInt(head.fieldIndex(_rankCol)) / rowsPerPartition).toString,
          "numPartitions" -> scala.math.max(head.getInt(head.fieldIndex(_partitionNumCol)), 1).toString
        ))
      } else {
        Iterator.empty
      }
    }.collectAsMap().toMap
  }
}
