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

import org.apache.druid.common.config.{NullHandling, NullValueHandlingConfig}

/**
  * Utility class for initializing a Druid NullValueHandlingConfig. In a Druid cluster, this is handled via injection
  * and in unit tests defautl value null handling is initialized via NullHandling.initializeForTests(), but we need
  * to use reflection here to handle the case where we want to set SQL-compatible null handling.
  */
object NullHandlingUtils {
  def initializeDruidNullHandling(useDruidDefaultHandling: Boolean): Unit = {
    if (useDruidDefaultHandling) {
      NullHandling.initializeForTests()
    } else {
      val nullHandlingConfig = new NullValueHandlingConfig(false)
      val instanceField = classOf[NullHandling].getDeclaredField("INSTANCE")
      instanceField.setAccessible(true)
      instanceField.set(null, nullHandlingConfig) // scalastyle:ignore null
    }
  }
}
