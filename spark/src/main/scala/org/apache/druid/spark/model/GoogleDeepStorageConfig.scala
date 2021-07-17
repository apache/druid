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

package org.apache.druid.spark.model

import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}

import scala.collection.mutable

class GoogleDeepStorageConfig extends DeepStorageConfig(DruidConfigurationKeys.googleDeepStorageTypeKey) {
  private val optionsMap: mutable.Map[String, String] = mutable.Map[String, String](
    DruidConfigurationKeys.deepStorageTypeKey -> deepStorageType
  )

  def bucket(bucket: String): GoogleDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.googleDeepStorageTypeKey,
      DruidConfigurationKeys.bucketKey)
    optionsMap.put(key, bucket)
    this
  }

  def prefix(prefix: String): GoogleDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.googleDeepStorageTypeKey,
      DruidConfigurationKeys.prefixKey)
    optionsMap.put(key, prefix)
    this
  }

  def maxListingLength(maxListingLength: Int): GoogleDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.googleDeepStorageTypeKey,
      DruidConfigurationKeys.maxListingLengthKey)
    optionsMap.put(key, maxListingLength.toString)
    this
  }

  override def toOptions: Map[String, String] = optionsMap.toMap
}
