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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.SignedBytes;

/**
 * An enum that provides a way for users to specify what encoding should be used when hashing strings.
 *
 * The main reason for this setting's existence is getting the best performance possible. When operating on memory
 * mapped segments -- which store strings as UTF-8 -- it is fastest to use "UTF8". When operating on the result of
 * expressions, or on an in-heap IncrementalIndex -- which use Java strings -- it is fastest to use "UTF16LE".
 *
 * This decision cannot be made locally, because different encodings do not generate equivalent hashes, and therefore
 * they are not mergeable. The decision must be made globally by the end user or by the SQL planner, and should be
 * based on where most input strings are expected to come from.
 *
 * Currently, UTF8 and UTF16LE are the only two options, because there are no situations where other options would be
 * higher-performing.
 */
public enum StringEncoding implements Cacheable
{
  // Do not change order; the ordinal is used by cache keys. Add new ones at the end.
  UTF8,
  UTF16LE /* Equivalent to treating the result of str.toCharArray() as a bag of bytes in little-endian order */;

  @JsonCreator
  public static StringEncoding fromString(final String name)
  {
    return valueOf(StringUtils.toUpperCase(name));
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{SignedBytes.checkedCast(ordinal())};
  }

  @JsonValue
  @Override
  public String toString()
  {
    return StringUtils.toLowerCase(this.name());
  }
}
