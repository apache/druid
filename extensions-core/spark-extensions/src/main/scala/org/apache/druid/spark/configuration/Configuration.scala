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

import org.apache.druid.java.util.common.StringUtils
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
  * A simple wrapper around a properties map that can also "dive" into a sub-Configuration if keys are stored using
  * dotted separators. Because this class is designed to make working with Spark DataSourceOptions easier and DSOs are
  * case-insensitive, this class is also case-insensitive.
  *
  * @param properties A Map of String property names to String property values to back this Configuration.
  */
class Configuration(properties: Map[String, String]) extends Serializable {
  def getAs[T](key: String): T = {
    properties(StringUtils.toLowerCase(key)).asInstanceOf[T]
  }

  def get(key: String): Option[String] = {
    properties.get(StringUtils.toLowerCase(key))
  }

  def get(keyWithDefault: (String, String)): String = {
    this.get(StringUtils.toLowerCase(keyWithDefault._1)).getOrElse(keyWithDefault._2)
  }

  def getString(key: String): String = {
    properties.getOrElse(StringUtils.toLowerCase(key), "")
  }

  def getInt(key: String, default: Int): Int = {
    properties.get(StringUtils.toLowerCase(key)).fold(default)(_.toInt)
  }

  def getInt(keyWithDefault: (String, Int)): Int = {
    this.getInt(StringUtils.toLowerCase(keyWithDefault._1), keyWithDefault._2)
  }

  def getLong(key: String, default: Long): Long = {
    properties.get(StringUtils.toLowerCase(key)).fold(default)(_.toLong)
  }

  def getLong(keyWithDefault: (String, Long)): Long = {
    this.getLong(StringUtils.toLowerCase(keyWithDefault._1), keyWithDefault._2)
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    properties.get(StringUtils.toLowerCase(key)).fold(default)(_.toBoolean)
  }

  def getBoolean(keyWithDefault: (String, Boolean)): Boolean = {
    this.getBoolean(StringUtils.toLowerCase(keyWithDefault._1), keyWithDefault._2)
  }

  def apply(key: String): String = {
    this.get(key) match {
      case Some(v) => v
      case None => throw new NoSuchElementException(s"Key $key not found!")
    }
  }

  def isPresent(key: String): Boolean = {
    properties.isDefinedAt(StringUtils.toLowerCase(key))
  }

  def isPresent(paths: String*): Boolean = {
    properties.isDefinedAt(StringUtils.toLowerCase(paths.mkString(Configuration.SEPARATOR)))
  }

  /**
    * Given a prefix PREFIX, return a Configuration object containing every key in this
    * configuration that starts with PREFIX. Keys in the resulting Configuration will have PREFIX
    * removed from their start.
    *
    * For example, if a Configuration contains the key `druid.broker.host` and `dive("druid")` is
    * called on it, the resulting Configuration will contain the key `broker.host` with the same
    * value as `druid.broker.host` had in the original Configuration.
    *
    * @param prefix The namespace to "dive" into.
    * @return A Configuration containing the keys in this Configuration that start with PREFIX,
    *         stripped of their leading PREFIX.
    */
  def dive(prefix: String): Configuration = {
    new Configuration(properties
      .filterKeys(_.startsWith(s"$prefix${Configuration.SEPARATOR}"))
      .map{case (k, v) => k.substring(prefix.length + 1) -> v})
  }

  /**
    * Given a number of prefixes, return a Configuration object containing every key in this
    * configuration that starts with `prefixes.mkString(Configuration.SEPARATOR)`. Keys in the
    * resulting Configuration will have the concatenation of PREFIXES removed from their start.
    *
    * Note that this is the equivalent of chaining `dive` calls, not chaining `merge` calls.
    *
    * @param prefixes The namespaces, in order, to "dive" into.
    * @return A Configuration containing the keys that start with every prefix in PREFIXES joined
    *         by periods, stripped of the leading prefixes matching the prefixes in PREFIXES.
    */
  def dive(prefixes: String*): Configuration = {
    prefixes.foldLeft(this){case (conf, prefix) => conf.dive(prefix)}
  }

  /**
    * Combine this configuration with another Configuration. If keys collide between these
    * configurations, the corresponding values in OTHER will be selected.
    *
    * @param other A Configuration to merge with this Configuration.
    * @return A Configuration containing the union of keys between this Configuration and OTHER.
    *         If keys collide between the two Configurations, the values in OTHER will be kept.
    */
  def merge(other: Configuration): Configuration = {
    new Configuration(this.properties ++ other.toMap)
  }

  /**
    * Combine this configuration with another Configuration, moving the other Configuration to the provided name space.
    * If keys collide between this configuration and the newly-namespaced OTHER, the corresponding values in OTHER will
    * be selected.
    *
    * @param namespace The name space to merge OTHER under.
    * @param other The Configuration to merge with this Configuration.
    * @return A new Configuration object containing all the keys in this Configuration, plus the keys in OTHER
    *         namespaced under NAMESPACE.
    */
  def merge(namespace: String, other: Configuration): Configuration = {
    this.merge(Configuration(namespace, other.toMap))
  }

  /**
    * Add the properties specified in PROPERTIES to this Configuration's properties, moving the new properties to the
    * provided name space. If this Configuration already contains keys under the provided name space and those keys
    * collide with the properties specified in PROPERTIES, the corresponding values in PROPERTIES will be selected.
    *
    * @param namespace The name space to merge the properties specified in PROPERTIES under.
    * @param properties The map of properties to values to combine with the properties from this Configuration.
    * @return A new Configuration object containing all the keys in this Configuration, plus the properties in
    *         PROPERTIES namespaced under NAMESPACE.
    */
  def merge(namespace: String, properties: Map[String, String]): Configuration = {
    this.merge(Configuration(namespace, properties))
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Configuration] && this.toMap == obj.asInstanceOf[Configuration].toMap
  }

  override def hashCode(): Int = {
    this.properties.hashCode()
  }

  def toMap: Map[String, String] = {
    this.properties
  }

  override def toString: String = {
    this.toMap.mkString("Configuration{", "; ", "}")
  }
}

object Configuration {
  def apply(properties: Map[String, String]): Configuration = {
    new Configuration(properties.map{case (k, v) => StringUtils.toLowerCase(k) -> v})
  }

  def apply(namespace: String, properties: Map[String, String]): Configuration = {
    new Configuration(properties.map{case(k, v) => StringUtils.toLowerCase(s"$namespace$SEPARATOR$k") -> v})
  }

  def apply(dso: DataSourceOptions): Configuration = {
    new Configuration(dso.asMap().asScala.toMap)
  }

  def fromKeyValue(key: String, value: String): Configuration = {
    new Configuration(Map[String, String](StringUtils.toLowerCase(key) -> value))
  }

  /**
    * Get the key corresponding to each element of PATHS interpreted as a namespace or property.
    *
    * @param paths The parent namespaces and property as individual strings to convert into a single configuration key.
    * @return The path to a property through its parent namespaces as a single configuration key.
    */
  def toKey(paths: String*): String = {
    StringUtils.toLowerCase(paths.mkString(Configuration.SEPARATOR))
  }

  private val SEPARATOR = "."
}
