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

package org.apache.druid.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

package object partitioners {
  private[partitioners] val timeBucketWindowSpec =
    (orderCol: String) => Window.partitionBy(_timeBucketCol).orderBy(col(orderCol))

  // Reserved column names used by in the DataFrame-based partitioners.
  private[partitioners] val _timeBucketCol = "__timeBucket"
  private[partitioners] val _rankCol = "__rank"
  private[partitioners] val _partitionNumCol = "__partitionNum"
  private[partitioners] val _partitionKeyCol = "__partitionKey"
}
