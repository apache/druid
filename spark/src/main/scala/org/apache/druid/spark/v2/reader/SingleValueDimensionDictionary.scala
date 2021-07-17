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

package org.apache.druid.spark.v2.reader

import org.apache.druid.java.util.common.StringUtils
import org.apache.spark.sql.execution.vectorized.Dictionary

/**
  * A Spark "Dictionary" to back single-valued dimension vectors (e.g. columns of strings).
  *
  * @param lookups A map of id to String to back this Dictionary with.
  */
class SingleValueDimensionDictionary(val lookups: Map[Int, String]) extends Dictionary {
  override def decodeToInt(id: Int): Int = {
    throw new UnsupportedOperationException("SingleValueDimensionDictionary encoding does not support ints!")
  }

  override def decodeToLong(id: Int): Long = {
    throw new UnsupportedOperationException("SingleValueDimensionDictionary encoding does not support longs!")
  }

  override def decodeToFloat(id: Int): Float = {
    throw new UnsupportedOperationException("SingleValueDimensionDictionary encoding does not support floats!")
  }

  override def decodeToDouble(id: Int): Double = {
    throw new UnsupportedOperationException("SingleValueDimensionDictionary encoding does not support doubles!")
  }

  override def decodeToBinary(id: Int): Array[Byte] = {
    // TODO: Figure out if we want toUtf8Nullable or toUtf8WithNullToEmpty. (This is likely related to adding support
    //  for Druid's two null-handling modes)
    StringUtils.toUtf8Nullable(lookups(id))
  }
}
