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

package org.apache.druid.spark.utils

import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

class DeepStorageConstructorHelpersSuite extends AnyFunSuite with Matchers {
  private val sampleLocalConf: Configuration = Configuration(Map[String, String](
    "deepStorageType" -> "local",
    "local.storageDirectory" -> "/tmp/working/directory"
  ))

  private val sampleHdfsConf: Configuration = Configuration(Map[String, String](
    "deepStorageType" -> "hdfs",
    "hdfs.storageDirectory" -> "/tmp/working/directory"
  ))

  private val sampleS3Conf: Configuration = Configuration(Map[String, String](
    "deepStorageType" -> "s3",
    "s3.bucket" -> "testBucket",
    "s3.baseKey" -> "prefix/to/druid",
    "s3.disableAcl" -> "true",
    "s3.maxListingLength" -> "999",
    "s3.useS3aSchema" -> "false",
    "s3.client.protocol" -> "https",
    "s3.accessKey" -> "my access key",
    "s3.secretKey" -> "my secret key",
    "s3.proxy.host" -> "proxy.host",
    "s3.proxy.port" -> "1234",
    "s3.proxy.username" -> "druid",
    "s3.proxy.password" -> "swordfish",
    "s3.client.disableChunkedEncoding" -> "true",
    "s3.endpoint.signingRegion" -> "us-west-1",
    "s3.sse.type" -> "kms",
    "s3.sse.kms.keyId" -> "key"
  ))

  private val sampleGoogleConfig: Configuration = Configuration(Map[String, String](
    "deepStorageType" -> "google",
    "google.bucket" -> "testBucket",
    "google.prefix" -> "prefix/to/druid",
    "google.maxListingLength" -> "1023"
  ))

  private val sampleAzureConfig: Configuration = Configuration(Map[String, String](
    "deepStorageType" -> "azure",
    "azure.account" -> "testAccount",
    "azure.key" -> "12345ABCDEF",
    "azure.container" -> "testContainer",
    "azure.prefix" -> "prefix/to/druid",
    "azure.maxListingLength" -> "1001"
  ))

  test("createLocalDataSegmentPusherConfig should construct a LocalDataSegmentPusherConfig") {
    val pusherConfig =DeepStorageConstructorHelpers.createLocalDataSegmentPusherConfig(
      sampleLocalConf.dive(DruidConfigurationKeys.localDeepStorageTypeKey)
    )
    pusherConfig.getStorageDirectory should equal(new File("/tmp/working/directory"))
  }

  test("createHdfsDataSegmentPusherConfig should construct an HdfsDataSegmentPusherConfig") {
    val pusherConfig = DeepStorageConstructorHelpers.createHdfsDataSegmentPusherConfig(
      sampleHdfsConf.dive(DruidConfigurationKeys.hdfsDeepStorageTypeKey)
    )
    pusherConfig.getStorageDirectory should equal("/tmp/working/directory")
  }

  test("createS3DataSegmentPusherConfig should construct an S3DataSegmentPusherConfig from a Configuration") {
    val pusherConfig = DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(
      sampleS3Conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey)
    )

    pusherConfig.getBucket should equal("testBucket")
    pusherConfig.getBaseKey should equal("prefix/to/druid")
    pusherConfig.getDisableAcl should be(true)
    pusherConfig.getMaxListingLength should equal(999)
    pusherConfig.isUseS3aSchema should be(false)

    val prunedMap = sampleS3Conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey).toMap - "uses3aschema" - "disableacl"
    val prunedConf = DeepStorageConstructorHelpers.createS3DataSegmentPusherConfig(Configuration(prunedMap))
    prunedConf.getDisableAcl should be(false)
    prunedConf.isUseS3aSchema should be(true)
  }

  test("createS3InputDataConfig should construct an S3InputDataConfig from a Configuration") {
    val inputDataConfig = DeepStorageConstructorHelpers.createS3InputDataConfig(
      sampleS3Conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey)
    )

    inputDataConfig.getMaxListingLength should equal(999)
  }

  test("prepareServerSideEncryptingAmazonS3 should correctly parse a Configuration") {
    val (credentialsConfig, proxyConfig, endpointConfig, clientConfig, _) =
      DeepStorageConstructorHelpers.createConfigsForServerSideEncryptingAmazonS3(
        sampleS3Conf.dive(DruidConfigurationKeys.s3DeepStorageTypeKey)
      )

    credentialsConfig.getAccessKey.getPassword should equal("my access key")
    proxyConfig.getPort should equal(1234)
    endpointConfig.getUrl should equal(null) // scalastyle:ignore null
    clientConfig.getProtocol should equal("https")
  }

  test("createS3StorageConfig should create objects of the correct type") {
    val noopSSEConfig = Configuration(Map[String, String]())
    val s3SSEConfig = Configuration(Map[String, String]("type" -> "s3"))
    val kmsSSEConfig = Configuration(Map[String, String]("type" -> "kms", "keyId" -> "key"))
    val customSSEConfig = Configuration(Map[String, String](
      "type" -> "custom",
      "custom.base64EncodedKey" -> "0123456789abcdef"
    ))

    val noopStorageConfig = DeepStorageConstructorHelpers.createS3StorageConfig(noopSSEConfig)
    val s3StorageConfig = DeepStorageConstructorHelpers.createS3StorageConfig(s3SSEConfig)
    val kmsStorageConfig = DeepStorageConstructorHelpers.createS3StorageConfig(kmsSSEConfig)
    val customStorageConfig = DeepStorageConstructorHelpers.createS3StorageConfig(customSSEConfig)

    // Just confirming the class names because the class is package-private and so not visible here.
    noopStorageConfig.getServerSideEncryption.getClass.getName should
      equal("org.apache.druid.storage.s3.NoopServerSideEncryption")
    s3StorageConfig.getServerSideEncryption.getClass.getName should
      equal("org.apache.druid.storage.s3.S3ServerSideEncryption")
    kmsStorageConfig.getServerSideEncryption.getClass.getName should
      equal("org.apache.druid.storage.s3.KmsServerSideEncryption")
    customStorageConfig.getServerSideEncryption.getClass.getName should
      equal("org.apache.druid.storage.s3.CustomServerSideEncryption")
  }

  test("createGoogleAcountConfig should correctly parse a Configuration") {
    val accountConfig = DeepStorageConstructorHelpers.createGoogleAcountConfig(
      sampleGoogleConfig.dive(DruidConfigurationKeys.googleDeepStorageTypeKey)
    )
    accountConfig.getBucket should equal("testBucket")
    accountConfig.getPrefix should equal("prefix/to/druid")
  }

  test("createGoogleInputDataConfig should correctly parse a Configuration") {
    val inputDataConfig = DeepStorageConstructorHelpers.createGoogleInputDataConfig(
      sampleGoogleConfig.dive(DruidConfigurationKeys.googleDeepStorageTypeKey)
    )
    inputDataConfig.getMaxListingLength should equal(1023)
  }

  test("createAzureDataSegmentConfig should correctly parse a Configuration") {
    val dataSegmentConfig = DeepStorageConstructorHelpers.createAzureDataSegmentConfig(
      sampleAzureConfig.dive(DruidConfigurationKeys.azureDeepStorageTypeKey)
    )
    dataSegmentConfig.getContainer should equal("testContainer")
    dataSegmentConfig.getPrefix should equal("prefix/to/druid")
  }

  test("createAzureInputDataConfig should correctly parse a Configuration") {
    val inputDataConfig = DeepStorageConstructorHelpers.createAzureInputDataConfig(
      sampleAzureConfig.dive(DruidConfigurationKeys.azureDeepStorageTypeKey)
    )
    inputDataConfig.getMaxListingLength should equal(1001)
  }

  test("createAzureAccountConfig should correctly parse a Configuration") {
    val accountConfig = DeepStorageConstructorHelpers.createAzureAccountConfig(
      sampleAzureConfig.dive(DruidConfigurationKeys.azureDeepStorageTypeKey)
    )
    accountConfig.getKey should equal("12345ABCDEF")
    accountConfig.getAccount should equal("testAccount")
    accountConfig.getProtocol should equal("https")
  }
}
