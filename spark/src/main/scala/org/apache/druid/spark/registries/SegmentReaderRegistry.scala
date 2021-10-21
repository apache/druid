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
import com.fasterxml.jackson.databind.InjectableValues
import com.fasterxml.jackson.databind.jsontype.NamedType
import org.apache.druid.guice.LocalDataStorageDruidModule
import org.apache.druid.java.util.common.{IAE, ISE, StringUtils}
import org.apache.druid.segment.loading.{LoadSpec, LocalDataSegmentPuller, LocalLoadSpec}
import org.apache.druid.spark
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.Configuration
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.DeepStorageConstructorHelpers
import org.apache.druid.storage.azure.{AzureByteSource, AzureByteSourceFactory, AzureDataSegmentPuller, AzureLoadSpec}
import org.apache.druid.storage.google.{GoogleDataSegmentPuller, GoogleLoadSpec, GoogleStorageDruidModule}
import org.apache.druid.storage.hdfs.{HdfsDataSegmentPuller, HdfsLoadSpec}
import org.apache.druid.storage.s3.{S3DataSegmentPuller, S3LoadSpec, S3StorageDruidModule}
import org.apache.druid.utils.CompressionUtils
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.Path

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}
import java.util.{Map => JMap}
import scala.collection.mutable

/**
  * A registry for functions to parse a "load spec" and load them into a provided file on an executor. `loadSpecType`
  * must match the LoadSpec's type name exactly.
  *
  * Users can also register initializers if necessary to set up injections and register Jackson subtypes. This
  * allows easier integration with deep storage types that aren't supported out
  * of the box, since an initializer function can just register the LoadSpec subtype used to
  * create a segment and allow Jackson to handle the rest. If custom logic is needed, a
  * registered load function will always take precedence.
  *
  * Note that DataSegment#getLoadSpec returns a Map<String, Object>, not an actual LoadSpec object.
  */
object SegmentReaderRegistry extends Logging {
  private val registeredSegmentLoaderFunctions: mutable.HashMap[String, (JMap[String, AnyRef], File) => Unit] =
    new mutable.HashMap()
  private val registeredInitializers: mutable.HashMap[String, (Configuration => Unit, Boolean)] =
    new mutable.HashMap()

  /**
    * Register functions to extract URIs from segment LoadSpecs. Functions should take a loadSpec (i.e. a Java Map from
    * String to AnyRef) and a destination file and pull the corresponding segment from deep storage to the file.
    *
    * @param loadSpecType The load spec type to register a load function for. Must match the value for the key
    *                     `loadSpecType` in the loadSpec map.
    * @param loadFunc A function that takes as its input a Java Map<String, Object> and a destination file and loads
    *                 the corresponding segment on deep storage to the file.
    */
  def registerLoadFunction(loadSpecType: String, loadFunc: (JMap[String, AnyRef], File) => Unit): Unit = {
    logInfo(s"Registering load function for deep storage type $loadSpecType")
    registeredSegmentLoaderFunctions(loadSpecType) = loadFunc
  }

  def registerInitializer(loadSpecType: String, initializeFunc: Configuration => Unit): Unit = {
    logInfo(s"Registering initializer for deep storage type $loadSpecType")
    registeredInitializers(loadSpecType) = (initializeFunc, false)
  }

  /**
    * Registers the default initializer function for DEEPSTORAGETYPE if one exists. This is a no-op
    * if there is no defined default initializer for DEEPSTORAGETYPE. Note as well that deep
    * storage type names may differ from Load Spec type names. In particular, the LoadSpec type
    * name for s3 deep storage is s3_zip.
    *
    * @param deepStorageType The deep storage type to register an initializer for.
    */
  def registerInitializerByType(deepStorageType: String): Unit = {
    if (!registeredInitializers.contains(deepStorageType)
      && knownInitializers.contains(deepStorageType)) {
      registerInitializer(deepStorageType, knownInitializers(deepStorageType))
    }
  }

  /**
    * Loads a segment according to the details in LOADSPEC to FILE. The rules for determining how
    * to load a segment are:
    *
    * 1. If no segment loader function or initializer has been registered, we attempt to construct
    *    a URI from LOADSPEC and then read that URI using FS. If the provided CONF is the Hadoop
    *    Configuration retrieved from SparkContext, we can defer all deep storage configuration
    *    and authorization to what the Spark cluster provides. Local, S3, HDFS, and GCS deep
    *    storages are supported.
    *
    * 2. If at least one segment loader function or initializer has been registered but no loader
    *    function has been registered for LOADSPEC's type, we delegate to Jackson to deserialize
    *    LOADSPEC into a LoadSpec object and then call #loadSegment(FILE) on the deserialized object.
    *    This requires LOADSPEC's type to have been registered with Jackson.
    *
    * 3. If we have registered a segment loader function for LOADSPEC's type, we use the registered
    *    function to load the segment into FILE. A segment loader function always takes precedence
    *    for its associated load spec type.
    *
    * @param loadSpec The LoadSpec for a segment.
    * @param file The file to load a segment to according to the properties in LOADSPEC.
    * @param conf The Hadoop configuration to use when reading segments from deep storage
    *             if no segment loader function or initializer is registered.
    */
  def load(loadSpec: JMap[String, AnyRef], file: File, conf: HConf): Unit = {
    if (registeredSegmentLoaderFunctions.isEmpty && registeredInitializers.isEmpty) {
      defaultLoad(loadSpec, file, conf)
    } else {
      val loadSpecType = loadSpec.get("type").toString
      if (!registeredSegmentLoaderFunctions.contains(loadSpecType)) {
        try {
          deserializeAndLoad(loadSpec, file)
        } catch {
          case e: Exception =>
            logError(s"Unable to deserialize ${MAPPER.writeValueAsString(loadSpec)} to a LoadSpec instance!", e)
            throw new IAE("No registered segment reader function or named LoadSpec subtype for load spec type %s",
              loadSpecType)
        }
      } else {
        registeredSegmentLoaderFunctions(loadSpecType)(loadSpec, file)
      }
    }
  }

  /**
    * Initializes a SegmentPuller for DEEPSTORAGETYPE based on CONF. CONF should have the deep storage
    * type prefix stripped away via .dive(DEEPSTORAGETYPE) to keep the extra object small.
    *
    * @param deepStorageType The deep storage type to initialize.
    * @param conf A Configuration object to provide user-supplied deep storage configuration properties.
    */
  def initialize(deepStorageType: String, conf: Configuration): Unit = {
    if (!registeredInitializers.contains(deepStorageType)) {
      if (knownInitializers.keySet.contains(deepStorageType)) {
        registerInitializerByType(deepStorageType)
      } else {
        throw new IAE("No registered initializer for deep storage type %s", deepStorageType)
      }
    }
    /*
     * This is synchronized to allow callers to do something like
     *   df.foreachPartition{_ => SegmentReaderRegistry.initialize("myType", conf)}
     *
     * The initialization functions themselves can be registered idempotently and so don't need to
     * be synchronized but should not be registered in the same .foreachPartition call or similar
     * (otherwise, each partition would reset the initialized flag and thus invoke initFunc multiple
     * times per executor).
     */
    registeredInitializers.synchronized{
      val (initFunc, init) = registeredInitializers(deepStorageType)
      if (!init) {
        initFunc(conf)
        registeredInitializers(deepStorageType) = (initFunc, true)
      }
    }
  }

  /**
    * A default load method adapted from JobHelper#getURIFromSegment. Loads a segment according to
    * LOADSPEC to FILE. This method assumes that any necessary authentication will be handled at
    * the machine instance and so needs no configuration. Additionally, this method requires the
    * target segment to load to be available at a URI constructable from LOADSPEC and so only
    * local, hdfs, gs, and s3 deep storages are supported.
    *
    * @param loadSpec The loadspec that describes where the segment to load should be read from.
    * @param file The file to read a segment into.
    * @param conf The Hadoop Configuration to use when constructing a filesystem to open the URI created from LOADSPEC.
    */
  private def defaultLoad(loadSpec: JMap[String, AnyRef], file: File, conf: HConf): Unit = {
    val loadSpecType = loadSpec.get("type").toString
    val uri = loadSpecType match {
      case LocalDataStorageDruidModule.SCHEME =>
        try {
          // scalastyle:off null
          new URI("file", null, loadSpec.get("path").toString, null, null)
          // scalastyle:on
        }
        catch {
          case e: URISyntaxException =>
            throw new ISE(e, "Unable to form simple file uri")
        }
      case "hdfs" => URI.create(loadSpec.get("path").toString)
      case GoogleStorageDruidModule.SCHEME =>
        // Segment names contain : in their path.
        // Google Cloud Storage supports : but Hadoop does not.
        // This becomes an issue when re-indexing using the current segments.
        // The Hadoop getSplits code doesn't understand the : and returns "Relative path in absolute URI"
        // This could be fixed using the same code that generates path names for hdfs segments using
        // getHdfsStorageDir. But that wouldn't fix this issue for people who already have segments with ":".
        // Because of this we just URL encode the : making everything work as it should.
        URI.create(StringUtils.format("gs://%s/%s", loadSpec.get("bucket"),
          StringUtils.replaceChar(loadSpec.get("path").toString, ':', "%3A")))
      case S3StorageDruidModule.SCHEME_S3_ZIP =>
        if ("s3a" == loadSpec.get("S3Schema")) {
          URI.create(StringUtils.format("s3a://%s/%s", loadSpec.get("bucket"),
            loadSpec.get("key")))
        } else {
          URI.create(StringUtils.format("s3n://%s/%s", loadSpec.get("bucket"),
            loadSpec.get("key")))

        }
    }

    val path = new Path(uri)
    val fs = path.getFileSystem(conf)
    try {
      CompressionUtils.unzip(fs.open(path), file)
    } catch {
      case exception@(_: IOException | _: RuntimeException) =>
        logError(s"Exception unzipping $path!", exception)
        throw exception
    }
  }

  private val knownInitializers: Map[String, Configuration => Unit] =
    Map[String, Configuration => Unit](
      LocalDataStorageDruidModule.SCHEME -> (_ => {
        val puller = new LocalDataSegmentPuller()
        val injectableValues = spark.injectableValues
          .addValue(classOf[LocalDataSegmentPuller], puller)
        MAPPER.setInjectableValues(injectableValues)
        MAPPER.registerSubtypes(classOf[LocalLoadSpec])
      }),
      "hdfs" -> ((conf: Configuration) => {
        val hadoopConfiguration = DeepStorageConstructorHelpers.createHadoopConfiguration(conf)
        val puller = new HdfsDataSegmentPuller(hadoopConfiguration)
        val injectableValues = spark.injectableValues
          .addValue(classOf[HdfsDataSegmentPuller], puller)
        MAPPER.setInjectableValues(injectableValues)
        MAPPER.registerSubtypes(new NamedType(classOf[HdfsLoadSpec], "hdfs"))
      }),
      GoogleStorageDruidModule.SCHEME -> (_ => {
        val googleStorage = DeepStorageConstructorHelpers.createGoogleStorage()
        val puller = new GoogleDataSegmentPuller(googleStorage)
        val injectableValues = spark.injectableValues
          .addValue(classOf[GoogleDataSegmentPuller], puller)
        MAPPER.setInjectableValues(injectableValues)
        MAPPER.registerSubtypes(classOf[GoogleLoadSpec])
      }),
      "s3" -> ((conf: Configuration) => {
        val s3 = DeepStorageConstructorHelpers.createServerSideEncryptingAmazonS3(conf)
        val puller = new S3DataSegmentPuller(s3)
        val injectableValues = spark.injectableValues
          .addValue(classOf[S3DataSegmentPuller], puller)
        MAPPER.setInjectableValues(injectableValues)
        MAPPER.registerSubtypes(classOf[S3LoadSpec])
      }),
      "azure" -> ((conf: Configuration) => {
        val azureStorage = DeepStorageConstructorHelpers.createAzureStorage(conf)
        val azureByteSourceFactory = new AzureByteSourceFactory {
          override def create(containerName: String, blobPath: String): AzureByteSource = {
            new AzureByteSource(azureStorage, containerName, blobPath)
          }
        }
        val puller = new AzureDataSegmentPuller(azureByteSourceFactory)
        val injectableValues = spark.injectableValues
          .addValue(classOf[AzureDataSegmentPuller], puller)
        MAPPER.setInjectableValues(injectableValues)
        MAPPER.registerSubtypes(classOf[AzureLoadSpec])
      })
    )

  private val deserializeAndLoad = (loadSpec: JMap[String, AnyRef], file: File) =>  {
    val loadSpecStr = MAPPER.writeValueAsString(loadSpec)
    MAPPER.readValue[LoadSpec](loadSpecStr, new TypeReference[LoadSpec] {}).loadSegment(file)
  }
}
