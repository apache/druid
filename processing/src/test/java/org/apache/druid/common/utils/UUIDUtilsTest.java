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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 *
 */
@RunWith(Parameterized.class)
public class UUIDUtilsTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> constructorFeeder()
  {
    final ArrayList<String[]> args = new ArrayList<>();
    final List<String> possibleArgs = Lists.newArrayList("one", "two", null, "");
    for (String possibleArg1 : possibleArgs) {
      args.add(new String[]{possibleArg1});
      for (String possibleArg2 : possibleArgs) {
        for (String possibleArg3 : possibleArgs) {
          args.add(new String[]{possibleArg1, possibleArg2, possibleArg3});
        }
      }
    }
    for (String possibleArg : possibleArgs) {
      args.add(new String[]{possibleArg});
    }
    return Collections2.transform(
        args,
        new Function<String[], Object[]>()
        {
          @Override
          public Object[] apply(String[] input)
          {
            final ArrayList<String> strings = new ArrayList<>(input.length);
            for (String str : input) {
              if (!Strings.isNullOrEmpty(str)) {
                strings.add(str);
              }
            }
            final String expected;
            if (!strings.isEmpty()) {
              expected = Joiner.on(UUIDUtils.UUID_DELIM).join(strings) + UUIDUtils.UUID_DELIM;
            } else {
              expected = "";
            }
            return new Object[]{input, expected};
          }
        }
    );
  }

  private final String[] args;
  private final String expectedBase;

  public UUIDUtilsTest(String[] args, String expectedBase)
  {
    this.args = args;
    this.expectedBase = expectedBase;
  }

  public static void validateIsStandardUUID(
      String uuidString
  )
  {
    // Since we strip the "-" from the string, we need to break them back out
    final ArrayList<String> strings = new ArrayList<>();
    strings.add(uuidString.substring(0, 8));
    strings.add(uuidString.substring(8, 12));
    strings.add(uuidString.substring(12, 16));
    strings.add(uuidString.substring(16, 20));
    strings.add(uuidString.substring(20, 32));
    UUID uuid = UUID.fromString(Joiner.on('-').join(strings));
    Assert.assertEquals(StringUtils.removeChar(uuid.toString(), '-'), uuidString);
  }

  @Test
  public void testUuid()
  {
    final String uuid = UUIDUtils.generateUuid(args);
    validateIsStandardUUID(uuid.substring(expectedBase.length()));
  }
}
