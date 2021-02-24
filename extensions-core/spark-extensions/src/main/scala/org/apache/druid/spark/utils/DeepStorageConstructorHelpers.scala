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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.InjectableValues
import com.microsoft.azure.storage.blob.{CloudBlobClient, ListBlobItem}
import com.microsoft.azure.storage.{StorageCredentials, StorageUri}
import org.apache.druid.common.aws.{AWSClientConfig, AWSCredentialsConfig, AWSEndpointConfig,
  AWSModule, AWSProxyConfig}
import org.apache.druid.common.gcp.GcpModule
import org.apache.druid.java.util.common.{IAE, StringUtils}
import org.apache.druid.metadata.{DefaultPasswordProvider, PasswordProvider}
import org.apache.druid.spark.MAPPER
import org.apache.druid.storage.azure.blob.{ListBlobItemHolder, ListBlobItemHolderFactory}
import org.apache.druid.storage.azure.{AzureAccountConfig, AzureCloudBlobIterable,
  AzureCloudBlobIterableFactory, AzureCloudBlobIterator, AzureCloudBlobIteratorFactory,
  AzureDataSegmentConfig, AzureInputDataConfig, AzureStorage}
import org.apache.druid.storage.google.{GoogleAccountConfig, GoogleInputDataConfig, GoogleStorage,
  GoogleStorageDruidModule}
import org.apache.druid.storage.s3.{NoopServerSideEncryption, S3DataSegmentPusherConfig,
  S3InputDataConfig, S3SSECustomConfig, S3SSEKmsConfig, S3StorageConfig, S3StorageDruidModule,
  ServerSideEncryptingAmazonS3, ServerSideEncryption}
import org.apache.hadoop.conf.Configuration

import java.io.{ByteArrayInputStream, DataInputStream}
import java.lang.{Iterable => JIterable}
import java.net.URI
import scala.collection.JavaConverters.asJavaIterableConverter

/**
  * This is a nested cesspit of miserableness hacked together in the odd hours of the night. Hopefully as these helpers
  * see actual use they can be refactored into a more decent approach.
  */
object DeepStorageConstructorHelpers extends TryWithResources {

  // HDFS Storage Helpers

  def createHadoopConfiguration(properties: Map[String, String]): Configuration = {
    val conf = new Configuration()
    val confByteStream = new ByteArrayInputStream(
      StringUtils.decodeBase64String(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.hdfsHadoopConfKey)))
    )
    tryWithResources(confByteStream, new DataInputStream(confByteStream)){
      case (_, inputStream: DataInputStream) => conf.readFields(inputStream)
    }
    conf
  }

  // S3 Storage Helpers

  def createS3DataSegmentPusherConfig(properties: Map[String, String]): S3DataSegmentPusherConfig = {
    val conf = new S3DataSegmentPusherConfig
    conf.setBaseKey(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3BaseKeyKey)))
    conf.setBucket(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.bucketKey)))
    conf.setDisableAcl(
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3DisableACLKey)).fold(false)(_.toBoolean)
    )
    val maxListingLength =
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey)).fold(1000)(_.toInt)
    // Although S3DataSegmentPusherConfig defaults to 1024, S3DataInputConfig requires maxListing length to be < 1000
    // This is another reason to move to name-spaced config keys.
    if (maxListingLength < 1 || maxListingLength > 1000) {
      throw new IAE("maxListingLength must be between 1 and 1000!")
    }
    conf.setMaxListingLength(maxListingLength)
    conf.setUseS3aSchema(properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3UseS3ASchemaKey)).fold(true)(_.toBoolean))
    conf
  }

  def createS3InputDataConfig(properties: Map[String, String]): S3InputDataConfig = {
    val inputDataConf = new S3InputDataConfig
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1000)(_.toInt)
    inputDataConf.setMaxListingLength(maxListingLength)
    inputDataConf
  }

  /**
    * No real clue if any of this or the sub-methods work ¯\_(ツ)_/¯
    */
  def createServerSideEncryptingAmazonS3(properties: Map[String, String]): ServerSideEncryptingAmazonS3 = {
    // TODO: Build a small Configuration utils class to support diving into sub-configurations and then standardize
    //  these property keys on the druid extensions props so that we can just pass the resulting sub-config into
    //  MAPPER.convertValue()
    val credentialsConfig = createAwsCredentialConfig(properties)

    val proxyConfig = createAwsProxyConfig(properties)

    val endpointConfig = createAwsEndpointConfig(properties)

    val clientConfig = createAwsClientConfig(properties)

    val s3StorageConfig = createS3StorageConfig(properties)

    val awsModule = new AWSModule
    val s3Module = new S3StorageDruidModule
    val credentialsProvider = awsModule.getAWSCredentialsProvider(credentialsConfig)
    s3Module.getAmazonS3Client(
      s3Module.getServerSideEncryptingAmazonS3Builder(
        credentialsProvider,
        proxyConfig,
        endpointConfig,
        clientConfig,
        s3StorageConfig
      )
    )
  }

  def createAwsCredentialConfig(properties: Map[String, String]): AWSCredentialsConfig = {
    if (
      properties.isDefinedAt(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsCredentialsConfigKey))
    ) {
      MAPPER.readValue[AWSCredentialsConfig](
        properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsCredentialsConfigKey)),
        new TypeReference[AWSCredentialsConfig] {}
      )
    } else {
      val credentialsProps = Map[String, AnyRef](
        "accessKey" ->
          MAPPER.readValue[PasswordProvider](
            properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AccessKeyKey), ""),
            new TypeReference[PasswordProvider] {}
          ),
        "secretKey" ->
          MAPPER.readValue[PasswordProvider](
            properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3SecretKeyKey), ""),
            new TypeReference[PasswordProvider] {}
          ),
        "fileSessionCredentials" ->
          properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3FileSessionCredentialsKey), "")
      )
      MAPPER.convertValue(credentialsProps, classOf[AWSCredentialsConfig])
    }
  }

  def createAwsProxyConfig(properties: Map[String, String]): AWSProxyConfig = {
    if (
      properties.isDefinedAt(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsProxyConfigKey))
    ) {
      MAPPER.readValue[AWSProxyConfig](
        properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsProxyConfigKey)),
        new TypeReference[AWSProxyConfig] {}
      )
    } else {
      val proxyProps = Map[String, AnyRef](
        "host" -> properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ProxyHostKey), ""),
        "port" -> properties
          .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ProxyPortKey))
          .fold(-1)(_.toInt)
          .asInstanceOf[Integer],
        "username" -> properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ProxyUserKey), ""),
        "password" -> properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ProxyPasswordKey), "")
      )
      MAPPER.convertValue(proxyProps, classOf[AWSProxyConfig])
    }
  }

  def createAwsEndpointConfig(properties: Map[String, String]): AWSEndpointConfig = {
    if (
      properties.isDefinedAt(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsEndpointConfigKey))
    ) {
      MAPPER.readValue[AWSEndpointConfig](
        properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsEndpointConfigKey)),
        new TypeReference[AWSEndpointConfig] {}
      )
    } else {
      val proxyProps = Map[String, AnyRef](
        "url" -> properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3EndpointConfigUrlKey)),
        "signingRegion" ->
          properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3EndpointConfigSigningRegionKey))
      )
      MAPPER.convertValue(proxyProps, classOf[AWSEndpointConfig])
    }
  }

  def createAwsClientConfig(properties: Map[String, String]): AWSClientConfig = {
    if (
      properties.isDefinedAt(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsClientConfigKey))
    ) {
      MAPPER.readValue[AWSClientConfig](
        properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3AwsClientConfigKey)),
        new TypeReference[AWSClientConfig] {}
      )
    } else {
      val proxyProps = Map[String, AnyRef](
        "protocol" ->
          properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ClientConfigProtocolKey), "https"),
        "disableChunkedEncoding" ->
          properties.get(
            StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ClientConfigDisableChunkedEncodingKey)
          ).fold(false)(_.toBoolean).asInstanceOf[java.lang.Boolean],
        "enablePathStyleAccess" ->
          properties.get(
            StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ClientConfigEnablePathStyleAccessKey)
          ).fold(false)(_.toBoolean).asInstanceOf[java.lang.Boolean],
        "forceGlobalBucketAccessEnabled" ->
          properties.get(
            StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ClientConfigForceGlobalBucketAccessEnabledKey)
          ).fold(false)(_.toBoolean).asInstanceOf[java.lang.Boolean]
      )
      MAPPER.convertValue(proxyProps, classOf[AWSClientConfig])
    }
  }

  def createS3StorageConfig(properties: Map[String, String]): S3StorageConfig = {
    if (
      properties.isDefinedAt(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3StorageConfigKey))
    ) {
      MAPPER.readValue[S3StorageConfig](
        properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3StorageConfigKey)),
        new TypeReference[S3StorageConfig] {}
      )
    } else {
      val storageConfigType = properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ServerSideEncryptionTypeKey))
      MAPPER.readValue[ServerSideEncryption]("s3", new TypeReference[ServerSideEncryption] {})
      val serverSideEncryption = storageConfigType match {
        case Some("s3") => MAPPER.readValue[ServerSideEncryption]("s3", new TypeReference[ServerSideEncryption] {})
        case Some("kms") =>
          val s3SseKmsConfig = MAPPER.convertValue(
            Map[String, AnyRef](
              "keyId" -> properties
                .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ServerSideEncryptionKmsKeyIdKey))),
            classOf[S3SSEKmsConfig]
          )
          MAPPER.setInjectableValues(new InjectableValues.Std().addValue(classOf[S3SSEKmsConfig], s3SseKmsConfig))
          MAPPER.readValue[ServerSideEncryption]("kms", new TypeReference[ServerSideEncryption] {})
        case Some("custom") =>
          val s3SseCustomConfig = MAPPER.convertValue(
            Map[String, AnyRef](
              "base64EncodedKey" -> properties
                .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ServerSideEncryptionCustomKeyKey))),
            classOf[S3SSECustomConfig]
          )
          MAPPER.setInjectableValues(new InjectableValues.Std().addValue(classOf[S3SSECustomConfig], s3SseCustomConfig))
          MAPPER.readValue[ServerSideEncryption]("custom", new TypeReference[ServerSideEncryption] {})
        case _ => new NoopServerSideEncryption
      }
      new S3StorageConfig(serverSideEncryption)
    }
  }

  // GCS Storage Helpers

  def createGoogleAcountConfig(properties: Map[String, String]): GoogleAccountConfig = {
    val accountConfig = new GoogleAccountConfig
    accountConfig.setBucket(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.bucketKey)))
    accountConfig.setPrefix(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.prefixKey)))
    accountConfig
  }

  def createGoogleInputDataConfig(properties: Map[String, String]): GoogleInputDataConfig = {
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val inputConfig = new GoogleInputDataConfig
    inputConfig.setMaxListingLength(maxListingLength)
    inputConfig
  }

  def createGoogleStorage(properties: Map[String, String]): GoogleStorage = {
    val gcpModule = new GcpModule
    val gcpStorageModule = new GoogleStorageDruidModule

    val httpTransport = gcpModule.getHttpTransport
    val jsonFactory = gcpModule.getJsonFactory
    val requestInitializer = gcpModule.getHttpRequestInitializer(httpTransport, jsonFactory)
    gcpStorageModule.getGoogleStorage(httpTransport, jsonFactory, requestInitializer)
  }

  // Azure Storage Helpers

  def createAzureDataSegmentConfig(properties: Map[String, String]): AzureDataSegmentConfig = {
    val dataSegmentConfig = new AzureDataSegmentConfig
    dataSegmentConfig.setContainer(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureContainerKey)))
    dataSegmentConfig.setPrefix(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.prefixKey)))
    dataSegmentConfig
  }

  def createAzureInputDataConfig(properties: Map[String, String]): AzureInputDataConfig = {
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val inputDataConfig = new AzureInputDataConfig
    inputDataConfig.setMaxListingLength(maxListingLength)
    inputDataConfig
  }

  def createAzureAccountConfig(properties: Map[String, String]): AzureAccountConfig = {
    val accountConfig = new AzureAccountConfig
    val maxTries = properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureMaxTriesKey), "3").toInt
    accountConfig.setProtocol(
      properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureProtocolKey), "https"))
    accountConfig.setMaxTries(maxTries)
    accountConfig.setAccount(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureAccountKey)))
    accountConfig.setKey(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureKeyKey)))
    accountConfig
  }

  def createAzureStorage(properties: Map[String, String]): AzureStorage = {
    val storageCredentials = StorageCredentials.tryParseCredentials(
      properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.connectionStringKey))
    )
    val primaryUri = properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azurePrimaryStorageUriKey))
      .map(new URI(_))
      .orNull
    val secondaryUri = properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureSecondaryStorageUriKey))
      .map(new URI(_))
      .orNull
    val storageUri = new StorageUri(primaryUri, secondaryUri)
    val cloudBlobClient = new CloudBlobClient(storageUri, storageCredentials)
    new AzureStorage(cloudBlobClient)
  }

  /**
    * I highly doubt this works, but I don't have an Azure system to test on nor do I have the familiarity with Azure
    * to be sure that testing with local mocks will actually test what I want to test.
    *
    * @param properties
    * @return
    */
  /*def createAzureCloudBlobIterableFactory(properties: Map[String, String]): AzureCloudBlobIterableFactory = {
    // Taking advantage of the fact that java.net.URIs deviates from spec and dissallow spaces to use it as a separator.
    val prefixes = properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azurePrefixesKey))
      .split(" ")
      .map(new URI(_))
      .toIterable
      .asJava
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val azureStorage = DeepStorageConstructorHelpers.createAzureStorage(properties)
    val accountConfig = DeepStorageConstructorHelpers.createAzureAccountConfig(properties)

    val listBlobItemHolderFactory = new ListBlobItemHolderFactory {
      override def create(blobItem: ListBlobItem): ListBlobItemHolder = new ListBlobItemHolder(blobItem)
    }

    // The constructor for AzureCloudBlobIterator is protected, so no dice here
    val azureCloudBlobIteratorFactory = new AzureCloudBlobIteratorFactory {
      override def create(ignoredPrefixes: JIterable[URI], ignoredMaxListingLength: Int): AzureCloudBlobIterator = {
        new AzureCloudBlobIterator(azureStorage, listBlobItemHolderFactory, accountConfig, prefixes, maxListingLength)
      }
    }
    (_: JIterable[URI], _: Int) => {
      new AzureCloudBlobIterable(azureCloudBlobIteratorFactory, prefixes, maxListingLength)
    }
  }*/
}
