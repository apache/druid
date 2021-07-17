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

import org.apache.druid.guice.LocalDataStorageDruidModule

import java.net.{URI, URISyntaxException}
import java.util.{Map => JMap}
import org.apache.druid.java.util.common.{IAE, ISE, StringUtils}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.storage.s3.S3StorageDruidModule

import scala.collection.mutable

/**
  * A registry for functions to translate a "load spec" into a URI for pulling onto an executor. `loadSpecType` must
  * match the LoadSpec's type name exactly.
  * Note that DataSegment#getLoadSpec returns a Map<String, Object>, not an actual LoadSpec object.
  */
object SegmentReaderRegistry extends Logging {
  private val registeredSegmentLoaderFunctions: mutable.HashMap[String, JMap[String, AnyRef] => URI] =
    new mutable.HashMap()

  /**
    * Register functions to extract URIs from segment LoadSpecs. Functions should take a loadSpec (i.e. a Java Map from
    * String to AnyRef) and return a URI that can be used to pull the corresponding segment from deep storage.
    *
    * @param loadSpecType The load spec type to register a load function for. Must match the value for the key
    *                     `loadSpecType` in the loadSpec map.
    * @param loadFunc A function that takes as its input a Java Map<String, Object> and returns a URI pointing to the
    *                 corresponding segment on deep storage.
    */
  def register(loadSpecType: String, loadFunc: JMap[String, AnyRef] => URI): Unit = {
    registeredSegmentLoaderFunctions(loadSpecType) = loadFunc
  }

  def registerByType(loadSpecType: String): Unit = {
    if (!registeredSegmentLoaderFunctions.contains(loadSpecType)
      && knownTypes.contains(loadSpecType)) {
      register(loadSpecType, knownTypes(loadSpecType))
    }
  }

  def load(loadSpec: JMap[String, AnyRef]): URI = {
    val loadSpecType = loadSpec.get("type").toString
    if (!registeredSegmentLoaderFunctions.contains(loadSpecType)) {
      if (knownTypes.keySet.contains(loadSpecType)) {
        registerByType(loadSpecType)
      } else {
        throw new IAE("No registered segment reader function for loadSpec %s",
          MAPPER.writeValueAsString(loadSpec))
      }
    }
    registeredSegmentLoaderFunctions(loadSpecType)(loadSpec)
  }

  private val knownTypes: Map[String, JMap[String, AnyRef] => URI] =
    Map[String, JMap[String, AnyRef] => URI](
      S3StorageDruidModule.SCHEME_S3_ZIP -> ((loadSpec: JMap[String, AnyRef]) =>
        if ("s3a" == loadSpec.get("S3Schema")) {
          URI.create(StringUtils.format("s3a://%s/%s", loadSpec.get("bucket"),
            loadSpec.get("key")))
        } else {
          URI.create(StringUtils.format("s3n://%s/%s", loadSpec.get("bucket"),
            loadSpec.get("key")))
        }),
      "hdfs" -> ((loadSpec: JMap[String, AnyRef]) => URI.create(loadSpec.get("path").toString)),
      "google" -> ((loadSpec: JMap[String, AnyRef]) =>
        // Segment names contain : in their path.
        // Google Cloud Storage supports : but Hadoop does not.
        // This becomes an issue when re-indexing using the current segments.
        // The Hadoop getSplits code doesn't understand the : and returns "Relative path in absolute URI"
        // This could be fixed using the same code that generates path names for hdfs segments using
        // getHdfsStorageDir. But that wouldn't fix this issue for people who already have segments with ":".
        // Because of this we just URL encode the : making everything work as it should.
        URI.create(StringUtils.format("gs://%s/%s", loadSpec.get("bucket"),
          StringUtils.replaceChar(loadSpec.get("path").toString, ':', "%3A")))),
      LocalDataStorageDruidModule.SCHEME -> ((loadSpec: JMap[String, AnyRef]) =>
        try {
          // scalastyle:off null
          new URI("file", null, loadSpec.get("path").toString, null, null)
          // scalastyle:on
        }
        catch {
          case e: URISyntaxException =>
            throw new ISE(e, "Unable to form simple file uri")
        }),
      "azure" -> ((loadSpec: JMap[String, AnyRef]) =>
        // I don't think this will work when we call CompressionUtils.unzip on a path made from this URI (it looks like
        // we'd need an AzureByteSource going by AzureEntity and AzureDataSegmentPuller)
        URI.create(StringUtils.format(
          "azure://%s/%s",
          loadSpec.get("containerName"),
          loadSpec.get("blobPath")
        )))
  )
}
