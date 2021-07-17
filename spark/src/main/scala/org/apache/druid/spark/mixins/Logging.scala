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

package org.apache.druid.spark.mixins

import org.slf4j.{Logger, LoggerFactory}

/**
  * Simplified version of org.apache.spark.internal.Logging.
  */
trait Logging {

  @transient private var logger: Logger = _

  private lazy val logName = this.getClass.getName.stripSuffix("$")

  /**
    * Return the configured underlying logger
    *
    * @return the configured underlying logger
    */
  protected def log: Logger = {
    if (logger == null) {
      logger = LoggerFactory.getLogger(logName)
    }
    logger
  }

  /**
    * Log a message at the TRACE level.
    *
    * @param msg the message string to be logged
    */
  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) {
      log.trace(msg)
    }
  }

  /**
    * Log a message at the DEBUG level.
    *
    * @param msg the message string to be logged
    */
  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) {
      log.debug(msg)
    }
  }

  /**
    * Log a message at the INFO level.
    *
    * @param msg the message string to be logged
    */
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) {
      log.info(msg)
    }
  }

  /**
    * Log a message at the WARN level.
    *
    * @param msg the message string to be logged
    */
  protected def logWarn(msg: => String): Unit = {
    if (log.isWarnEnabled) {
      log.warn(msg)
    }
  }

  /**
    * Log a message with an exception at the WARN level.
    *
    * @param msg       the message string to be logged
    * @param exception the exception to log in addition to the message
    */
  protected def logWarn(msg: => String, exception: Throwable): Unit = {
    if (log.isWarnEnabled) {
      log.warn(msg, exception)
    }
  }

  /**
    * Log a message at the ERROR level.
    *
    * @param msg the message string to be logged
    */
  protected def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) {
      log.error(msg)
    }
  }

  /**
    * Log a message with an exception at the ERROR level.
    *
    * @param msg       the message string to be logged
    * @param exception the exception to log in addition to the message
    */
  protected def logError(msg: => String, exception: Throwable): Unit = {
    if (log.isErrorEnabled) {
      log.error(msg, exception)
    }
  }
}
