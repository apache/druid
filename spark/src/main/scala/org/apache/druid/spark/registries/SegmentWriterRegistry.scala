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

import org.apache.druid.java.util.common.IAE
import org.apache.druid.segment.loading.{DataSegmentKiller, DataSegmentPusher,
  LocalDataSegmentKiller, LocalDataSegmentPusher}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.DeepStorageConstructorHelpers
import org.apache.druid.storage.azure.{AzureDataSegmentKiller, AzureDataSegmentPusher}
import org.apache.druid.storage.google.{GoogleDataSegmentKiller, GoogleDataSegmentPusher}
import org.apache.druid.storage.hdfs.{HdfsDataSegmentKiller, HdfsDataSegmentPusher}
import org.apache.druid.storage.s3.{S3DataSegmentKiller, S3DataSegmentPusher}

import scala.collection.mutable

/**
  * A registry for functions to create DataSegmentPushers and DataSegmentKillers. These pushers and killers are used
  * to write Druid segments to deep storage and kill aborted segments already written to deep storage, respectively.
  */
object SegmentWriterRegistry extends Logging {
  private val registeredSegmentPusherCreatorFunctions: mutable.HashMap[String, Configuration =>
    DataSegmentPusher] = new mutable.HashMap()
  private val registeredSegmentKillerCreatorFunctions: mutable.HashMap[String, Configuration =>
    DataSegmentKiller] = new mutable.HashMap()

  /**
    * Register segment pusher and segment killer creator functions for the given deep storage type. Both creator
    * functions should take a Configuration object and return a DataSegmentPusher or DataSegmentKiller, respectively.
    *
    * @param deepStorageType The deep storage type to register segment pusher and killer creation functions for.
    * @param segmentPusherCreatorFunc A function that takes as its only input a Configuration object and returns a
    *                                 DataSegmentPusher instancce capable of pushing segments to the given deep storage.
    * @param segmentKillerCreatorFunc A function that takes as its only input a Configuration object and returns a
    *                                 DataSegmentKiller instancce capable of killing segments on the given deep storage.
    */
  def register(
                deepStorageType: String,
                segmentPusherCreatorFunc: Configuration => DataSegmentPusher,
                segmentKillerCreatorFunc: Configuration => DataSegmentKiller
              ): Unit = {
    registeredSegmentPusherCreatorFunctions(deepStorageType) = segmentPusherCreatorFunc
    registeredSegmentKillerCreatorFunctions(deepStorageType) = segmentKillerCreatorFunc
  }

  /**
    * Shortcut function for registering known segment writing plugins by type.
    *
    * @param deepStorageType The deep storage type to register segment writing plugins for.
    */
  def registerByType(deepStorageType: String): Unit = {
    if (!registeredSegmentPusherCreatorFunctions.contains(deepStorageType)
      && knownTypes.contains(deepStorageType)) {
      knownTypes(deepStorageType)()
    }
  }

  /**
    * Returns a DataSegmentPusher capable of pushing Druid segments to the specified deep storage type. DEEPSTORAGETYPE
    * must either be known (e.g. defined in SegmentWriterRegistry.knownTypes) or previously registered.
    *
    * @param deepStorageType The deep storage type to create a DataSegmentPusher for.
    * @param properties A Configuration object containing the properties provided when dataframe.write() was called.
    *                   Used to configure the returned DataSegmentPusher if necessary.
    * @return A DataSegmentPusher capable of pushing segments to the deep storage type referenced by DEEPSTORAGETYPE and
    *         configured based on PROPERTIES.
    */
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

  /**
    * Returns a DataSegmentKiller capable of deleting Druid segments on the specified deep storage type. DEEPSTORAGETYPE
    * must either be known (e.g. defined in SegmentWriterRegistry.knownTypes) or previously registered.
    *
    * @param deepStorageType The deep storage type to create a DataSegmentKiller for.
    * @param properties A Configuration object containing the properties provided when dataframe.write() was called.
    *                   Used to configure the returned DataSegmentKiller if necessary.
    * @return A DataSegmentPusher capable of deleting segments on the deep storage type referenced by DEEPSTORAGETYPE
    *         and configured based on PROPERTIES.
    */
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
      DruidConfigurationKeys.azureDeepStorageTypeKey -> (
        () => register(
          DruidConfigurationKeys.azureDeepStorageTypeKey,
          (conf: Configuration) => {
            val azureConf = conf.dive(DruidConfigurationKeys.azureDeepStorageTypeKey)

            new AzureDataSegmentPusher(
              DeepStorageConstructorHelpers.createAzureStorage(azureConf),
              DeepStorageConstructorHelpers.createAzureAccountConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureDataSegmentConfig(azureConf)
            )
          },
          (conf: Configuration) => {
            val azureConf = conf.dive(DruidConfigurationKeys.azureDeepStorageTypeKey)

            new AzureDataSegmentKiller(
              DeepStorageConstructorHelpers.createAzureDataSegmentConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureInputDataConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureAccountConfig(azureConf),
              DeepStorageConstructorHelpers.createAzureStorage(azureConf),
              /* AzureCloudBlobIterableFactory is only used for AzureDataSegmentKiller.killAll(), which we don't call */
              null // scalastyle:ignore null
            )
          }
        )
      )
    )
}
