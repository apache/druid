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

package org.apache.druid.spark.v2.reader

import org.apache.druid.spark.SparkFunSuite
import org.apache.druid.spark.configuration.SerializableHadoopConfiguration
import org.apache.spark.sql.vectorized.ColumnarBatch

class DruidColumnarInputPartitionReaderSuite extends SparkFunSuite with InputPartitionReaderBehaviors[ColumnarBatch] {

  private val conf = () => sparkContext.broadcast(new SerializableHadoopConfiguration(sparkContext.hadoopConfiguration))

  testsFor(
    inputPartitionReader(
      "DruidColumnarInputPartitionReader with batch size 1",
      conf,
      new DruidColumnarInputPartitionReader(_, _, _, _, _, _, _, batchSize = 1),
      columnarPartitionReaderToSeq
    )
  )

  testsFor(
    inputPartitionReader(
      "DruidColumnarInputPartitionReader with batch size 2",
      conf,
      new DruidColumnarInputPartitionReader(_, _, _, _, _, _, _, batchSize = 2),
      columnarPartitionReaderToSeq
    )
  )

  testsFor(
    inputPartitionReader(
      "DruidColumnarInputPartitionReader with batch size 512",
      conf,
      new DruidColumnarInputPartitionReader(_, _, _, _, _, _, _, batchSize = 512),
      columnarPartitionReaderToSeq
    )
  )
}
