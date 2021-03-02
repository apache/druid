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

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.IAE
import org.apache.druid.segment.loading.{DataSegmentKiller, DataSegmentPusher,
  LocalDataSegmentKiller, LocalDataSegmentPusher}
import org.apache.druid.spark.utils.{Configuration, DeepStorageConstructorHelpers,
  DruidConfigurationKeys, Logging}
import org.apache.druid.spark.MAPPER
import org.apache.druid.storage.azure.{AzureCloudBlobIterableFactory, AzureDataSegmentKiller,
  AzureDataSegmentPusher}
import org.apache.druid.storage.google.{GoogleDataSegmentKiller, GoogleDataSegmentPusher}
import org.apache.druid.storage.hdfs.{HdfsDataSegmentKiller, HdfsDataSegmentPusher}
import org.apache.druid.storage.s3.{S3DataSegmentKiller, S3DataSegmentPusher}

import scala.collection.mutable

/**
  * A registry for functions to create DataSegmentPushers and DataSegmentKillers.
  */
object SegmentWriterRegistry extends Logging {
  private val registeredSegmentPusherCreatorFunctions: mutable.HashMap[String, Configuration =>
    DataSegmentPusher] = new mutable.HashMap()
  private val registeredSegmentKillerCreatorFunctions: mutable.HashMap[String, Configuration =>
    DataSegmentKiller] = new mutable.HashMap()

  def register(
                deepStorageType: String,
                segmentPusherCreatorFunc: Configuration => DataSegmentPusher,
                segmentKillerCreatorFunc: Configuration => DataSegmentKiller
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
                        properties: Configuration
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
                        properties: Configuration
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
      DruidConfigurationKeys.localDeepStorageTypeKey -> (
        () =>
          register(
            DruidConfigurationKeys.localDeepStorageTypeKey,
            (conf: Configuration) => {
              val localConf = conf.dive(DruidConfigurationKeys.localDeepStorageTypeKey)
              new LocalDataSegmentPusher(
                DeepStorageConstructorHelpers.createLocalDataSegmentPusherConfig(localConf)
              )
            },
            (conf: Configuration) => {
              val localConf = conf.dive(DruidConfigurationKeys.localDeepStorageTypeKey)
              new LocalDataSegmentKiller(
                DeepStorageConstructorHelpers.createLocalDataSegmentPusherConfig(localConf)
              )
            }
          )
        ),
      DruidConfigurationKeys.hdfsDeepStorageTypeKey -> (
        () =>
          register(
            DruidConfigurationKeys.hdfsDeepStorageTypeKey,
            (conf: Configuration) => {
              val hdfsConf = conf.dive(DruidConfigurationKeys.hdfsDeepStorageTypeKey)
              new HdfsDataSegmentPusher(
                DeepStorageConstructorHelpers.createHdfsDataSegmentPusherConfig(hdfsConf),
                DeepStorageConstructorHelpers.createHadoopConfiguration(hdfsConf),
                MAPPER
            )
            },
            (conf: Configuration) => {
              val hdfsConf = conf.dive(DruidConfigurationKeys.hdfsDeepStorageTypeKey)
              new HdfsDataSegmentKiller(
                DeepStorageConstructorHelpers.createHadoopConfiguration(hdfsConf),
                DeepStorageConstructorHelpers.createHdfsDataSegmentPusherConfig(hdfsConf)
              )
            }
          )
      ),
      DruidConfigurationKeys.s3DeepStorageTypeKey -> (
        () => register(
          DruidConfigurationKeys.s3DeepStorageTypeKey,
          (conf: Configuration) => {
            val s3Conf = conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey)
            new S3DataSegmentPusher(
              DeepStorageConstructorHelpers.createServerSideEncryptingAmazonS3(s3Conf),
              DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(s3Conf))
          },
          (conf: Configuration) => {
            val s3Conf = conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey)
            new S3DataSegmentKiller(
              DeepStorageConstructorHelpers.createServerSideEncryptingAmazonS3(s3Conf),
              DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(s3Conf),
              DeepStorageConstructorHelpers.createS3InputDataConfig(s3Conf)
            )
          }
        )
      ),
      DruidConfigurationKeys.googleDeepStorageTypeKey -> (
        () => register(
          DruidConfigurationKeys.googleDeepStorageTypeKey,
          (conf: Configuration) => {
            val googleConf = conf.dive(DruidConfigurationKeys.googleDeepStorageTypeKey)
            new GoogleDataSegmentPusher(
              DeepStorageConstructorHelpers.createGoogleStorage(),
              DeepStorageConstructorHelpers.createGoogleAcountConfig(googleConf)
            )
          },
          (conf: Configuration) => {
            val googleConf = conf.dive(DruidConfigurationKeys.googleDeepStorageTypeKey)

            new GoogleDataSegmentKiller(
              DeepStorageConstructorHelpers.createGoogleStorage(),
              DeepStorageConstructorHelpers.createGoogleAcountConfig(googleConf),
              DeepStorageConstructorHelpers.createGoogleInputDataConfig(googleConf)
            )
          }
        )
      ),
      DruidConfigurationKeys.azureDeepStorageKey -> (
        () => register(
          DruidConfigurationKeys.azureDeepStorageKey,
          (conf: Configuration) => {
            val azureConf = conf.dive(DruidConfigurationKeys.azureDeepStorageKey)

            new AzureDataSegmentPusher(
              DeepStorageConstructorHelpers.createAzureStorage(azureConf),
              DeepStorageConstructorHelpers.createAzureAccountConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureDataSegmentConfig(azureConf)
            )
          },
          (conf: Configuration) => {
            val azureConf = conf.dive(DruidConfigurationKeys.azureDeepStorageKey)
            /*val azureCloudBlobIterableFactory =
              DeepStorageConstructorHelpers.createAzureCloudBlobIterableFactory(properties)*/

            new AzureDataSegmentKiller(
              DeepStorageConstructorHelpers.createAzureDataSegmentConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureInputDataConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureAccountConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureStorage(azureConf),
              MAPPER.readValue[AzureCloudBlobIterableFactory](
                conf.getString(DruidConfigurationKeys.azureCloudBlobIterableFactoryConfigKey),
                new TypeReference[AzureCloudBlobIterableFactory] {}
              )
            )
          }
        )
      )
    )
}
