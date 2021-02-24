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

import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.java.util.common.parsers.TimestampParser
import org.apache.druid.spark.MAPPER
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType}

import java.sql.Timestamp

/**
  * A mix-in trait for providing a mapping between a Spark partition and the necessary information to construct a Druid
  * segment. This trait is most useful when composed with a Spark Partitioner.
  */
trait PartitionMapProvider {
  def getPartitionMap: Map[Int, Map[String, String]]
}

object PartitionMapProvider {
  /**
    * Given a map PARTITIONMAP from Spark partition id to a map of properties, serialize the map in a way the writer
    * knows how to deserialize.
    *
    * @param partitionMap The map to serialize
    * @return A serialized representation of PARTITIONMAP.
    */
  def serializePartitionMap(partitionMap: Map[Int, Map[String, String]]): String = {
    MAPPER.writeValueAsString(partitionMap)
  }

  /**
    * Utility function to convert a DataFrame DF into a bucketed RDD with rows having the form
    * (bucketStartMs -> map(col -> value)). The bucket start time is determined by parsing TSCOL in DF according to
    * the format specified in TSFORMAT and then bucketing according to GRANULARITY. Generally speaking, if this
    * bucketing can be done while processing the source data frame earlier in the execution graph, it should be.
    *
    * @param df The input DataFrame to bucket.
    * @param tsCol The column in DF to parse timestamps from.
    * @param tsFormat The format of the timestamp contained in TSCOL.
    * @param granularityStr The granularity to bucket the timestamps derived from TSCOL and TSFORMAT from.
    * @return An RDD consisting of mapping between a bucket start timestamp and the row the bucket was parsed from.
    */
  def bucketDf(
                df: DataFrame,
                tsCol: String,
                tsFormat: String,
                granularityStr: String
              ): RDD[(Long, Map[String, Any])] = {
    require(df.schema.fieldNames.contains(tsCol), s"Input df does not contain the provided timestamp column $tsCol!")
    val tsColIndex = df.schema.fieldIndex(tsCol)
    val tsClass = getClassFromSparkDataType(df.schema(tsCol).dataType)
    val fields = df.schema.fieldNames
    df.rdd.mapPartitions{i =>
      val parser = TimestampParser.createObjectTimestampParser(tsFormat)
      val granularity = Granularity.fromString(granularityStr)
      df.schema(tsCol).dataType
      i.map{row =>
        val ts = parser.apply(row.get(tsColIndex).asInstanceOf[tsClass.type])
        val bucketMs = granularity.bucketStart(ts).getMillis
        bucketMs -> row.getValuesMap[Any](fields)
      }
    }
  }

  /**
    * Given an RDD containing pairs of bucket and rows, return the number of rows in each bucket. Approximate algorithms
    * should be used if performance is a concern.
    *
    * @param rdd An RDD of (bucket, row) pairs.
    * @return A mapping between bucket and the number of rows in that bucket.
    */
  def getCountByBucket(rdd: RDD[(Long, Map[String, Any])]): Map[Long, Long] = {
    rdd.countByKey().toMap
  }

  private[partitioners] def getClassFromSparkDataType(dt: DataType): Class[_] = {
    dt match {
      case DoubleType => classOf[Double]
      case FloatType => classOf[Float]
      case IntegerType => classOf[Int]
      case LongType => classOf[Long]
      case StringType => classOf[String]
      case TimestampType => classOf[Timestamp]
    }
  }
}
