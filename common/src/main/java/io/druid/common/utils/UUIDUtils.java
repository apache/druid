/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.UUID;

/**
 *
 */
public class UUIDUtils
{
  public static final String UUID_DELIM = "_";

  /**
   * Generates a universally unique identifier.
   *
   * @param extraData Extra data which often takes the form of debugging information
   *
   * @return A string which is a universally unique id (as determined by java.util.UUID) with extra data. It does not conform to a UUID variant standard.
   */
  public static String generateUuid(String... extraData)
  {
    String extra = null;
    if (extraData != null && extraData.length > 0) {
      final ArrayList<String> extraStrings = new ArrayList<>(extraData.length);
      for (String extraString : extraData) {
        if (!Strings.isNullOrEmpty(extraString)) {
          extraStrings.add(extraString);
        }
      }
      if (!extraStrings.isEmpty()) {
        extra = Joiner.on(UUID_DELIM).join(extraStrings);
      }
    }
    final String uuid = UUID.randomUUID().toString().replace("-", ""); // We don't use "-" in general, so remove them here.
    return extra == null ? uuid : (extra + UUID_DELIM + uuid);
  }
}
