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

package org.apache.druid.spark.configuration

import org.apache.spark.sql.sources.v2.DataSourceOptions

object DruidConfigurationKeys {
  // Shadowing DataSourceOptions.TABLE_KEY here so other classes won't need unnecessary imports
  val tableKey: String = DataSourceOptions.TABLE_KEY

  // Metadata Client Configs
  val metadataPrefix: String = "metadata"
  val metadataDbTypeKey: String = "dbType"
  val metadataHostKey: String = "host"
  val metadataPortKey: String = "port"
  val metadataConnectUriKey: String = "connectUri"
  val metadataUserKey: String = "user"
  val metadataPasswordKey: String = "password"
  val metadataDbcpPropertiesKey: String = "dbcpProperties"
  val metadataBaseNameKey: String = "baseName"
  private[spark] val metadataHostDefaultKey: (String, String) = (metadataHostKey, "localhost")
  private[spark] val metadataPortDefaultKey: (String, Int) = (metadataPortKey, 1527)
  private[spark] val metadataBaseNameDefaultKey: (String, String) = (metadataBaseNameKey, "druid")

  // Druid Client Configs
  val brokerPrefix: String = "broker"
  val brokerHostKey: String = "host"
  val brokerPortKey: String = "port"
  val numRetriesKey: String = "numRetries"
  val retryWaitSecondsKey: String = "retryWaitSeconds"
  val timeoutMillisecondsKey: String = "timeoutMilliseconds"
  private[spark] val brokerHostDefaultKey: (String, String) = (brokerHostKey, "localhost")
  private[spark] val brokerPortDefaultKey: (String, Int) = (brokerPortKey, 8082)
  private[spark] val numRetriesDefaultKey: (String, Int) = (numRetriesKey, 5)
  private[spark] val retryWaitSecondsDefaultKey: (String, Int) = (retryWaitSecondsKey, 5)
  private[spark] val timeoutMillisecondsDefaultKey: (String, Int) = (timeoutMillisecondsKey, 300000)

  // Common configs
  val useCompactSketchesKey: String = "useCompactSketches" // Default: false
  val useDefaultValueForNull: String = "useDefaultValueForNull" // Default: true
  private[spark] val useCompactSketchesDefaultKey: (String, Boolean) = (useCompactSketchesKey, false)
  private[spark] val useDefaultValueForNullDefaultKey: (String, Boolean) = (useDefaultValueForNull, true)

  // Reader Configs
  val readerPrefix: String = "reader"
  val segmentsKey: String = "segments"
  val vectorizeKey: String = "vectorize" // Default: false, experimental key!
  val batchSizeKey: String = "batchSize" // Default: 512
  val useSparkConfForDeepStorageKey: String = "useClusterConfForDeepStorage"
  private[spark] val vectorizeDefaultKey: (String, Boolean) = (vectorizeKey, false)
  private[spark] val batchSizeDefaultKey: (String, Int) = (batchSizeKey, 512)
  private[spark] val useSparkConfForDeepStorageDefaultKey: (String, Boolean) = (useSparkConfForDeepStorageKey, false)


  val deepStorageTypeKey: String = "deepStorageType"
  private[spark] val deepStorageTypeDefaultKey: (String, String) = (deepStorageTypeKey, "local")

  // Common Deep Storage Configs
  val storageDirectoryKey: String = "storageDirectory"
  val bucketKey: String = "bucket"
  val maxListingLengthKey: String = "maxListingLength"
  val prefixKey: String = "prefix"
  val protocolKey: String = "protocol"

  // Local Deep Storage Configs
  val localDeepStorageTypeKey: String = "local"
  val localStorageDirectoryKey: String = "storageDirectory"

  // HDFS Deep Storage Configs
  val hdfsDeepStorageTypeKey: String = "hdfs"
  val hdfsHadoopConfKey: String = "hadoopConf" // Base64-encoded serialized Configuration

  // S3 Deep Storage Configs
  val s3DeepStorageTypeKey: String = "s3"
  val s3BaseKeyKey: String = "baseKey"
  val s3DisableAclKey: String = "disableAcl"
  val s3UseS3ASchemaKey: String = "useS3aSchema"
  val s3AccessKeyKey: String = "accessKey"
  val s3SecretKeyKey: String = "secretKey"
  val s3FileSessionCredentialsKey: String = "fileSessionCredentials"
  val s3ProxyPrefix: String = "proxy"
  val s3ProxyHostKey: String = "host"
  val s3ProxyPortKey: String = "port"
  val s3ProxyUsernameKey: String = "username"
  val s3ProxyPasswordKey: String = "password"
  val s3EndpointPrefix: String = "endpoint"
  val s3EndpointUrlKey: String = "url"
  val s3EndpointSigningRegionKey: String = "signingRegion"
  val s3DisableChunkedEncodingKey: String = "disableChunkedEncoding"
  val s3EnablePathStyleAccessKey: String = "enablePathStyleAccess"
  val s3ForceGlobalBucketAccessEnabledKey: String = "forceGlobalBucketAccessEnabled"
  val s3ServerSideEncryptionPrefix: String = "sse"
  val s3ServerSideEncryptionTypeKey: String = "type"
  val s3ServerSideEncryptionKmsKeyIdKey: String = "keyId"
  val s3ServerSideEncryptionCustomKeyKey: String = "base64EncodedKey"

  // GCS Deep Storage Configs
  val googleDeepStorageTypeKey: String = "google"

  // Azure Deep Storage Configs
  val azureDeepStorageTypeKey: String = "azure"
  val azureAccountKey: String = "account"
  val azureKeyKey: String = "key"
  val azureMaxTriesKey: String = "maxTries"
  val azureContainerKey: String = "container"
}
