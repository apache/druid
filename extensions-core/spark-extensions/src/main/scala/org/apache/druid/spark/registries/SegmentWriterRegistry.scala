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

package org.apache.druid.spark.registries

import java.io.File
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.guice.LocalDataStorageDruidModule
import org.apache.druid.java.util.common.{IAE, StringUtils}
import org.apache.druid.segment.loading.{DataSegmentKiller, DataSegmentPusher,
  LocalDataSegmentKiller, LocalDataSegmentPusher, LocalDataSegmentPusherConfig}
import org.apache.druid.spark.utils.{DeepStorageConstructorHelpers, DruidDataSourceOptionKeys, Logging}
import org.apache.druid.spark.MAPPER
import org.apache.druid.storage.azure.{AzureCloudBlobIterableFactory, AzureDataSegmentKiller,
  AzureDataSegmentPusher}
import org.apache.druid.storage.google.{GoogleDataSegmentKiller, GoogleDataSegmentPusher, GoogleStorage}
import org.apache.druid.storage.hdfs.{HdfsDataSegmentKiller, HdfsDataSegmentPusher,
  HdfsDataSegmentPusherConfig}
import org.apache.druid.storage.s3.{S3DataSegmentKiller, S3DataSegmentPusher,
  S3StorageDruidModule, ServerSideEncryptingAmazonS3}
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

/**
  * A registry for functions to create DataSegmentPushers and DataSegmentKillers.
  */
object SegmentWriterRegistry extends Logging {
  private val registeredSegmentPusherCreatorFunctions: mutable.HashMap[String, Map[String, String] =>
    DataSegmentPusher] = new mutable.HashMap()
  private val registeredSegmentKillerCreatorFunctions: mutable.HashMap[String, DataSourceOptions =>
    DataSegmentKiller] = new mutable.HashMap()

  def register(
                deepStorageType: String,
                segmentPusherCreatorFunc: Map[String, String] => DataSegmentPusher,
                segmentKillerCreatorFunc: DataSourceOptions => DataSegmentKiller
              ): Unit = {
    registeredSegmentPusherCreatorFunctions(deepStorageType) = segmentPusherCreatorFunc
    registeredSegmentKillerCreatorFunctions(deepStorageType) = segmentKillerCreatorFunc
  }

  def registerByType(deepStorageType: String): Unit = {
    if (!registeredSegmentPusherCreatorFunctions.contains(deepStorageType)
      && knownTypes.contains(deepStorageType)) {
      knownTypes(deepStorageType)()
    }
  }

  def getSegmentPusher(
                        deepStorageType: String,
                        properties: Map[String, String]
                      ): DataSegmentPusher = {
    if (!registeredSegmentPusherCreatorFunctions.keySet.contains(deepStorageType)) {
      if (knownTypes.keySet.contains(deepStorageType)) {
        registerByType(deepStorageType)
      } else {
        throw new IAE("No registered segment pusher creation function for deep storage " +
          "type %s", deepStorageType)
      }
    }
    registeredSegmentPusherCreatorFunctions(deepStorageType)(properties)
  }

  def getSegmentKiller(
                        deepStorageType: String,
                        properties: DataSourceOptions
                      ): DataSegmentKiller = {
    if (!registeredSegmentKillerCreatorFunctions.keySet.contains(deepStorageType)) {
      if (knownTypes.keySet.contains(deepStorageType)) {
        registerByType(deepStorageType)
      } else {
        throw new IAE("No registered segment killer creation function for deep storage " +
          "type %s", deepStorageType)
      }
    }
    registeredSegmentKillerCreatorFunctions(deepStorageType)(properties)
  }

  private val knownTypes: Map[String, () => Unit] =
    Map[String, () => Unit](
      LocalDataStorageDruidModule.SCHEME -> (
        () =>
          register(
            LocalDataStorageDruidModule.SCHEME,
            (properties: Map[String, String]) =>
              new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig() {
                // DataSourceOptions are case-insensitive, so when we use the map form we need to lowercase keys
                override def getStorageDirectory: File =
                  new File(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.storageDirectoryKey)))
              }),
            (dataSourceOptions: DataSourceOptions) =>
              new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig() {
                override def getStorageDirectory: File =
                  new File(dataSourceOptions.get(DruidDataSourceOptionKeys.storageDirectoryKey).get)
              })
          )
        ),
      // HdfsStorageDruidModule.SCHEME is package-private, so we can't access it here
      DruidDataSourceOptionKeys.hdfsDeepStorageTypeKey -> (
        () =>
          register(
            DruidDataSourceOptionKeys.hdfsDeepStorageTypeKey,
            (properties: Map[String, String]) => {
              val pusherConfig = new HdfsDataSegmentPusherConfig
              pusherConfig.setStorageDirectory(
                properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.storageDirectoryKey))
              )
              val conf = DeepStorageConstructorHelpers.createHadoopConfiguration(properties)

              new HdfsDataSegmentPusher(
                pusherConfig,
                conf,
                MAPPER
            )
            },
            (dataSourceOptions: DataSourceOptions) => {
              val properties = dataSourceOptions.asMap().asScala.toMap
              val pusherConfig = new HdfsDataSegmentPusherConfig
              pusherConfig.setStorageDirectory(dataSourceOptions.get(DruidDataSourceOptionKeys.hdfsHadoopConfKey).get)
              val conf = DeepStorageConstructorHelpers.createHadoopConfiguration(properties)

              new HdfsDataSegmentKiller(
                conf,
                pusherConfig
              )
            }
          )
      ),
      S3StorageDruidModule.SCHEME -> (
        () => register(
          S3StorageDruidModule.SCHEME,
          (properties: Map[String, String]) => {
            new S3DataSegmentPusher(
              DeepStorageConstructorHelpers.createServerSideEncryptingAmazonS3(properties),
              DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(properties))
          },
          (dataSourceOptions: DataSourceOptions) => {
            val properties = dataSourceOptions.asMap().asScala.toMap
            new S3DataSegmentKiller(
              DeepStorageConstructorHelpers.createServerSideEncryptingAmazonS3(properties),
              DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(properties),
              DeepStorageConstructorHelpers.createS3InputDataConfig(properties)
            )
          }
        )
      ),
      // GoogleStorageDruidModule.SCHEME is package-private as well
      DruidDataSourceOptionKeys.googleDeepStorageTypeKey -> (
        () => register(
          DruidDataSourceOptionKeys.googleDeepStorageTypeKey,
          (properties: Map[String, String]) => {

            new GoogleDataSegmentPusher(
              DeepStorageConstructorHelpers.createGoogleStorage(properties),
              DeepStorageConstructorHelpers.createGoogleAcountConfig(properties)
            )
          },
          (dataSourceOptions: DataSourceOptions) => {
            val properties = dataSourceOptions.asMap().asScala.toMap

            new GoogleDataSegmentKiller(
              DeepStorageConstructorHelpers.createGoogleStorage(properties),
              DeepStorageConstructorHelpers.createGoogleAcountConfig(properties),
              DeepStorageConstructorHelpers.createGoogleInputDataConfig(properties)
            )
          }
        )
      ),
      // AzureStorageDruidModule is package-private
      DruidDataSourceOptionKeys.azureDeepStorageKey -> (
        () => register(
          DruidDataSourceOptionKeys.azureDeepStorageKey,
          (properties: Map[String, String]) => {
            val azureStorage = DeepStorageConstructorHelpers.createAzureStorage(properties)
            val accountConfig = DeepStorageConstructorHelpers.createAzureAccountConfig(properties)
            val segmentConfig = DeepStorageConstructorHelpers.createAzureDataSegmentConfig(properties)

            new AzureDataSegmentPusher(
              azureStorage,
              accountConfig,
              segmentConfig
            )
          },
          (dataSourceOptions: DataSourceOptions) => {
            val properties = dataSourceOptions.asMap().asScala.toMap
            val segmentConfig = DeepStorageConstructorHelpers.createAzureDataSegmentConfig(properties)
            val inputDataConfig = DeepStorageConstructorHelpers.createAzureInputDataConfig(properties)
            val accountConfig = DeepStorageConstructorHelpers.createAzureAccountConfig(properties)
            val azureStorage = DeepStorageConstructorHelpers.createAzureStorage(properties)
            /*val azureCloudBlobIterableFactory =
              DeepStorageConstructorHelpers.createAzureCloudBlobIterableFactory(properties)*/

            new AzureDataSegmentKiller(
              segmentConfig,
              inputDataConfig,
              accountConfig,
              azureStorage,
              MAPPER.readValue[AzureCloudBlobIterableFactory](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureCloudBlobIterableFactoryConfigKey).get,
                new TypeReference[AzureCloudBlobIterableFactory] {}
              )
            )
          }
        )
      )
    )
}
