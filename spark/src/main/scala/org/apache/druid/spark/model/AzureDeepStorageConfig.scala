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

import org.apache.druid.metadata.DynamicConfigProvider
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}

import scala.collection.mutable

class AzureDeepStorageConfig extends DeepStorageConfig(DruidConfigurationKeys.azureDeepStorageTypeKey) {
  private val optionsMap: mutable.Map[String, String] = mutable.Map[String, String](
    DruidConfigurationKeys.deepStorageTypeKey -> deepStorageType
  )

  def account(account: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.azureAccountKey)
    optionsMap.put(key, account)
    this
  }

  def key(keyProvider: DynamicConfigProvider[String], confKey: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.azureKeyKey)
    optionsMap.put(key, keyProvider.getConfig.getOrDefault(confKey, ""))
    this
  }

  def key(azureKey: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.azureKeyKey)
    optionsMap.put(key, azureKey)
    this
  }

  def maxTries(maxTries: Int): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.azureMaxTriesKey)
    addToOptions(key, maxTries)
    this
  }

  def protocol(protocol: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.protocolKey)
    optionsMap.put(key, protocol)
    this
  }

  def container(container: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.azureContainerKey)
    optionsMap.put(key, container)
    this
  }

  def maxListingLength(maxListingLength: Int): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.maxListingLengthKey)
    optionsMap.put(key, maxListingLength.toString)
    this
  }

  def prefix(prefix: String): AzureDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.azureDeepStorageTypeKey,
      DruidConfigurationKeys.prefixKey)
    optionsMap.put(key, prefix)
    this
  }

  override def toOptions: Map[String, String] = optionsMap.toMap

  private def addToOptions(key: String, value: Any): AzureDeepStorageConfig = {
    optionsMap.put(key, value.toString)
    this
  }
}
