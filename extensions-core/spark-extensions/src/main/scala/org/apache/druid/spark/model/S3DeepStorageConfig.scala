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

import org.apache.druid.metadata.PasswordProvider
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}

import scala.collection.mutable

class S3DeepStorageConfig extends DeepStorageConfig(DruidConfigurationKeys.s3DeepStorageTypeKey) {
  private val optionsMap: mutable.Map[String, String] = mutable.Map[String, String](
    DruidConfigurationKeys.deepStorageTypeKey -> deepStorageType
  )

  def bucket(bucket: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.bucketKey), bucket)
  }

  def baseKey(baseKey: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3BaseKeyKey), baseKey)
  }

  def maxListingLength(maxListingLength: Int): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.bucketKey), maxListingLength)
  }

  def disableAcl(disableAcl: Boolean): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3DisableAclKey), disableAcl)
  }

  def useS3aSchema(useS3aSchema: Boolean): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3UseS3ASchemaKey), useS3aSchema)
  }

  def accessKey(accessKey: PasswordProvider): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3AccessKeyKey), MAPPER.writeValueAsString(accessKey))
  }

  def accessKey(accessKey: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3AccessKeyKey), accessKey)
  }

  def secretKey(secretKey: PasswordProvider): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3SecretKeyKey), MAPPER.writeValueAsString(secretKey))
  }

  def secretKey(secretKey: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3AccessKeyKey), secretKey)
  }

  def fileSessionCredentials(fileSessionCredentials: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3FileSessionCredentialsKey), fileSessionCredentials)
  }

  def proxyHost(host: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProxyPrefix, DruidConfigurationKeys.s3ProxyHostKey), host)
  }

  def proxyPort(port: Int): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProxyPrefix, DruidConfigurationKeys.s3ProxyPortKey), port.toString)
  }

  def proxyUsername(username: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProxyPrefix, DruidConfigurationKeys.s3ProxyUsernameKey), username)
  }

  def proxyPassword(password: PasswordProvider): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProxyPrefix, DruidConfigurationKeys.s3ProxyPasswordKey),
      MAPPER.writeValueAsString(password))
  }

  def proxyPassword(password: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProxyPrefix, DruidConfigurationKeys.s3ProxyPasswordKey), password)
  }

  def endpointUrl(endpointUrl: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3EndpointPrefix, DruidConfigurationKeys.s3EndpointUrlKey), endpointUrl)
  }

  def endpointSigningRegion(endpointSigningRegion: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3EndpointPrefix, DruidConfigurationKeys.s3EndpointSigningRegionKey),
      endpointSigningRegion
    )
  }

  def protocol(protocol: String): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ProtocolKey), protocol)
  }

  def disableChunkedEnconding(disableChunkedEncoding: Boolean): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3DisableChunkedEncodingKey), disableChunkedEncoding)
  }

  def enablePathStyleAcess(enablePathStyleAccess: Boolean): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3EnablePathStyleAccessKey), enablePathStyleAccess)
  }

  def forceGlobalBucketAccessEnabled(forceGlobalBucketAccessEnabled: Boolean): S3DeepStorageConfig = {
    addToOptions(prefix(DruidConfigurationKeys.s3ForceGlobalBucketAccessEnabledKey), forceGlobalBucketAccessEnabled)
  }

  def sseType(sseType: String): S3DeepStorageConfig = {
    addToOptions(prefix(
      DruidConfigurationKeys.s3ServerSideEncryptionPrefix, DruidConfigurationKeys.s3ServerSideEncryptionTypeKey),
      sseType
    )
  }

  def sseKmsKeyId(sseKmsKeyId: String): S3DeepStorageConfig = {
    addToOptions(prefix(
      DruidConfigurationKeys.s3ServerSideEncryptionPrefix, DruidConfigurationKeys.s3ServerSideEncryptionKmsKeyIdKey),
      sseKmsKeyId
    )
  }

  def sseCustomKey(sseCustomKey: String): S3DeepStorageConfig = {
    addToOptions(prefix(
      DruidConfigurationKeys.s3ServerSideEncryptionPrefix, DruidConfigurationKeys.s3ServerSideEncryptionCustomKeyKey),
      sseCustomKey
    )
  }

  override def toOptions: Map[String, String] = optionsMap.toMap

  private def addToOptions(key: String, value: Any): S3DeepStorageConfig = {
    optionsMap.put(key, value.toString)
    this
  }

  private def prefix(keys: String*): String = {
    Configuration.toKey(DruidConfigurationKeys.s3DeepStorageTypeKey +: keys:_*)
  }
}
