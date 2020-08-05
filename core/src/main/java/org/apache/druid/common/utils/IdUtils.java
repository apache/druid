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

package org.apache.druid.common.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdUtils
{
  private static final Pattern INVALIDCHARS = Pattern.compile("(?s).*[^\\S ].*");

  private static final Joiner UNDERSCORE_JOINER = Joiner.on("_");

  public static void validateId(String thingToValidate, String stringToValidate)
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(stringToValidate),
        StringUtils.format("%s cannot be null or empty. Please provide a %s.", thingToValidate, thingToValidate)
    );
    Preconditions.checkArgument(
        !stringToValidate.startsWith("."),
        StringUtils.format("%s cannot start with the '.' character.", thingToValidate)
    );
    Preconditions.checkArgument(
        !stringToValidate.contains("/"),
        StringUtils.format("%s cannot contain the '/' character.", thingToValidate)
    );
    Matcher m = INVALIDCHARS.matcher(stringToValidate);
    Preconditions.checkArgument(
        !m.matches(),
        StringUtils.format("%s cannot contain whitespace character except space.", thingToValidate)
    );
  }

  public static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((ThreadLocalRandom.current().nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  public static String getRandomIdWithPrefix(String prefix)
  {
    return UNDERSCORE_JOINER.join(prefix, IdUtils.getRandomId());
  }
}
