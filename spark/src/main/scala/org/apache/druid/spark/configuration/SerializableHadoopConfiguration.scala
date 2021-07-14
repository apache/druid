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

import org.apache.druid.spark.mixins.Logging
import org.apache.hadoop.conf.{Configuration => HConf}

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import scala.util.control.NonFatal

/**
  * Adapted from org.apache.spark.util.SerializableConfiguration
  * A serializable version of a hadoop Configuration. Use `value` to access the Configuration.
  *
  * @param value Hadoop configuration
  */
class SerializableHadoopConfiguration(@transient var value: HConf) extends Serializable
  with Logging {
  private def writeObject(out: ObjectOutputStream): Unit = {
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    try {
      value = new HConf(false)
      value.readFields(in)
    } catch  {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
}
