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

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.introspect.AnnotatedClass
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.druid.common.aws.{AWSClientConfig, AWSCredentialsConfig, AWSEndpointConfig,
  AWSModule, AWSProxyConfig}
import org.apache.druid.common.gcp.GcpModule
import org.apache.druid.java.util.common.StringUtils
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.mixins.TryWithResources
import org.apache.druid.storage.azure.{AzureAccountConfig, AzureDataSegmentConfig,
  AzureInputDataConfig, AzureStorage, AzureStorageDruidModule}
import org.apache.druid.storage.google.{GoogleAccountConfig, GoogleInputDataConfig, GoogleStorage,
  GoogleStorageDruidModule}
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig
import org.apache.druid.storage.s3.{NoopServerSideEncryption, S3DataSegmentPusherConfig,
  S3InputDataConfig, S3SSECustomConfig, S3SSEKmsConfig, S3StorageConfig, S3StorageDruidModule,
  ServerSideEncryptingAmazonS3, ServerSideEncryption}
import org.apache.hadoop.conf.{Configuration => HConf}

import java.io.{ByteArrayInputStream, DataInputStream}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object DeepStorageConstructorHelpers extends TryWithResources {
  /*
   * Spark DataSourceOption property maps are case insensitive, by which they mean they lower-case all keys. Since all
   * our user-provided property keys will come to us via a DataSourceOption, we need to use a case-insensisitive jackson
   * mapper to deserialize property maps into objects. We want to be case-aware in the rest of our code, so we create a
   * private, case-insensitive copy of our mapper here.
   */
  private val caseInsensitiveMapper = MAPPER.copy()
    .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
    .registerModule(DefaultScalaModule)

  // Local Storage Helpers

  def createLocalDataSegmentPusherConfig(conf: Configuration): LocalDataSegmentPusherConfig = {
    convertConfToInstance(conf, classOf[LocalDataSegmentPusherConfig])
  }

  // HDFS Storage Helpers

  def createHdfsDataSegmentPusherConfig(conf: Configuration): HdfsDataSegmentPusherConfig = {
    convertConfToInstance(conf, classOf[HdfsDataSegmentPusherConfig])
  }

  def createHadoopConfiguration(conf: Configuration): HConf = {
    val hadoopConf = new HConf()
    val confByteStream = new ByteArrayInputStream(
      StringUtils.decodeBase64String(conf.getString(DruidConfigurationKeys.hdfsHadoopConfKey))
    )
    tryWithResources(confByteStream, new DataInputStream(confByteStream)){
      case (_, inputStream: DataInputStream) => hadoopConf.readFields(inputStream)
    }
    hadoopConf
  }

  // S3 Storage Helpers

  /**
    * Create an S3DataSegmentPusherConfig from the relevant properties in CONF.
    *
    * *** Note that we explicitly override the default for `useS3aSchema`! ***
    * Almost all users will want to use s3a, not s3n, and we have no backwards-compatibility to maintain.
    *
    * @param conf The Configuration object specifying the S3DataSegmentPusherConfig to create.
    * @return An S3DataSegmentPusherConfig derived from the properties specified in CONF.
    */
  def createS3DataSegmentPusherConfig(conf: Configuration): S3DataSegmentPusherConfig = {
    if (!conf.isPresent(DruidConfigurationKeys.s3UseS3ASchemaKey)) {
      convertConfToInstance(conf.merge(
        Configuration.fromKeyValue(DruidConfigurationKeys.s3UseS3ASchemaKey, "true")
      ), classOf[S3DataSegmentPusherConfig])
    } else {
      convertConfToInstance(conf, classOf[S3DataSegmentPusherConfig])
    }
  }

  def createS3InputDataConfig(conf: Configuration): S3InputDataConfig = {
    convertConfToInstance(conf, classOf[S3InputDataConfig])
  }

  def createServerSideEncryptingAmazonS3(conf: Configuration): ServerSideEncryptingAmazonS3 = {
    val (credentialsConfig, proxyConfig, endpointConfig, clientConfig, s3StorageConfig) =
      createConfigsForServerSideEncryptingAmazonS3(conf)

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

  def createConfigsForServerSideEncryptingAmazonS3(conf: Configuration):
  (AWSCredentialsConfig, AWSProxyConfig, AWSEndpointConfig, AWSClientConfig, S3StorageConfig) = {
    val credentialsConfig = convertConfToInstance(conf, classOf[AWSCredentialsConfig])

    val proxyConfig = convertConfToInstance(conf.dive("proxy"), classOf[AWSProxyConfig])

    val endpointConfig = convertConfToInstance(conf.dive("endpoint"), classOf[AWSEndpointConfig])

    val clientConfig = convertConfToInstance(conf.dive("client"), classOf[AWSClientConfig])

    val s3StorageConfig = createS3StorageConfig(conf.dive(DruidConfigurationKeys.s3ServerSideEncryptionPrefix))
    (credentialsConfig, proxyConfig, endpointConfig, clientConfig, s3StorageConfig)
  }

  /**
    * A helper method for creating instances of S3StorageConfigs from a Configuration. While I'm sure there's a simple
    * solution I'm missing, I would have thought that something like the following would have worked:
    *
    * ```
    * val kmsConfig = convertConfToInstance(conf.dive("kms"), classOf[S3SSEKmsConfig])
    * caseInsensitiveMapper.setInjectableValues(new InjectableValues.Std().addValue(classOf[S3SSEKmsConfig], kmsConfig))
    * val ser = caseInsensitiveMapper.writeValueAsString(Map[String, String]("type" -> "kms"))
    * caseInsensitiveMapper.readValue[ServerSideEncryption](ser, new TypeReference[ServerSideEncryption] {})
    * ```
    *
    * However, the code above throws an com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Invalid
    * definition for property `config` (of type `org.apache.druid.storage.s3.KmsServerSideEncryption`): Could not find
    * creator property with name 'config' (known Creator properties: [])
    *
    * I _think_ that the root cause is that ServerSideEncryption is abstract, but the error message above isn't
    * what I would expect. Nevertheless, the simple solution would be to serialize to a KmsServerSideEncryption
    * instance and then cast to the base ServerSideEncryption to assign. Unfortunately, KmsServerSideEncryption
    * is package-private, so we can't access the class here. Since we already have the config object and we
    * need to muck about with field visibility, we take the shortcut and just make the constructor accessible. This
    * solution generalizes to the CustomServerSideEncyption case as well.
    */
  def createS3StorageConfig(conf: Configuration): S3StorageConfig = {
    // There's probably a more elegant way to do this that would allow us to transparently support new sse types, but
    // this will work for now.
    val sseType = conf.get(DruidConfigurationKeys.s3ServerSideEncryptionTypeKey)

    // Getting the list of subtypes since we'll need to use it to grab references to the package-private implementations
    val config = caseInsensitiveMapper.getDeserializationConfig
    val ac = AnnotatedClass.constructWithoutSuperTypes(classOf[ServerSideEncryption], config)
    val subtypes = caseInsensitiveMapper.getSubtypeResolver.collectAndResolveSubtypesByClass(config, ac)

    val serverSideEncryption: ServerSideEncryption = sseType match {
      case Some("s3") =>
        val clazz = subtypes.asScala.filter(_.getName == "s3").head.getType
        val constructor = clazz.getDeclaredConstructor()
        constructor.setAccessible(true)
        constructor.newInstance().asInstanceOf[ServerSideEncryption]
      case Some("kms") =>
        val kmsConfig = convertConfToInstance(conf.dive("kms"), classOf[S3SSEKmsConfig])
        val clazz = subtypes.asScala.filter(_.getName == "kms").head.getType
        val constructor = clazz.getDeclaredConstructor(classOf[S3SSEKmsConfig])
        constructor.setAccessible(true)
        constructor.newInstance(kmsConfig).asInstanceOf[ServerSideEncryption]
      case Some("custom") =>
        val customConfig = convertConfToInstance(conf.dive("custom"), classOf[S3SSECustomConfig])
        val clazz = subtypes.asScala.filter(_.getName == "custom").head.getType
        val constructor = clazz.getDeclaredConstructor(classOf[S3SSECustomConfig])
        constructor.setAccessible(true)
        constructor.newInstance(customConfig).asInstanceOf[ServerSideEncryption]
      case _ => new NoopServerSideEncryption
    }
    new S3StorageConfig(serverSideEncryption)
  }

  // GCS Storage Helpers

  def createGoogleAcountConfig(conf: Configuration): GoogleAccountConfig = {
    convertConfToInstance(conf, classOf[GoogleAccountConfig])
  }

  def createGoogleInputDataConfig(conf: Configuration): GoogleInputDataConfig = {
    convertConfToInstance(conf, classOf[GoogleInputDataConfig])
  }

  def createGoogleStorage(): GoogleStorage = {
    val gcpModule = new GcpModule
    val gcpStorageModule = new GoogleStorageDruidModule

    val httpTransport = gcpModule.getHttpTransport
    val jsonFactory = gcpModule.getJsonFactory
    val requestInitializer = gcpModule.getHttpRequestInitializer(httpTransport, jsonFactory)
    gcpStorageModule.getGoogleStorage(httpTransport, jsonFactory, requestInitializer)
  }

  // Azure Storage Helpers

  def createAzureDataSegmentConfig(conf: Configuration): AzureDataSegmentConfig = {
    convertConfToInstance(conf, classOf[AzureDataSegmentConfig])
  }

  def createAzureInputDataConfig(conf: Configuration): AzureInputDataConfig = {
    convertConfToInstance(conf, classOf[AzureInputDataConfig])
  }

  def createAzureAccountConfig(conf: Configuration): AzureAccountConfig = {
    convertConfToInstance(conf, classOf[AzureAccountConfig])
  }

  def createAzureStorage(conf: Configuration): AzureStorage = {
    val accountConfig = convertConfToInstance(conf, classOf[AzureAccountConfig])
    val azureModule = new AzureStorageDruidModule
    val cloudBlobClient = azureModule.getCloudBlobClient(accountConfig)
    azureModule.getAzureStorageContainer(cloudBlobClient)
  }

  private def convertConfToInstance[T](conf: Configuration, clazz: Class[T]): T = {
    caseInsensitiveMapper.convertValue(conf.toMap, clazz)
  }
}
