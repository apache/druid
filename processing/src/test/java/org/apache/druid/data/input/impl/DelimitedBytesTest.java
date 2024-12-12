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

package org.apache.druid.data.input.impl;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class DelimitedBytesTest extends InitializedNullHandlingTest
{
  private static final byte TSV = (byte) '\t';

  @Test
  public void testEmpty()
  {
    Assert.assertEquals(
        Collections.singletonList(NullHandling.sqlCompatible() ? null : ""),
        DelimitedBytes.split(new byte[0], TSV, DelimitedBytes.UNKNOWN_FIELD_COUNT)
    );
  }

  @Test
  public void testNoDelimiter()
  {
    Assert.assertEquals(
        Collections.singletonList("abc"),
        DelimitedBytes.split(StringUtils.toUtf8("abc"), TSV, DelimitedBytes.UNKNOWN_FIELD_COUNT)
    );
  }

  @Test
  public void testOneDelimiter()
  {
    Assert.assertEquals(
        Arrays.asList("a", "bc"),
        DelimitedBytes.split(StringUtils.toUtf8("a\tbc"), TSV, DelimitedBytes.UNKNOWN_FIELD_COUNT)
    );
  }

  @Test
  public void testDelimiterAtStart()
  {
    Assert.assertEquals(
        Arrays.asList(NullHandling.sqlCompatible() ? null : "", "abc"),
        DelimitedBytes.split(StringUtils.toUtf8("\tabc"), TSV, DelimitedBytes.UNKNOWN_FIELD_COUNT)
    );
  }

  @Test
  public void testDelimiterAtEnd()
  {
    Assert.assertEquals(
        Arrays.asList("a", "bc", NullHandling.sqlCompatible() ? null : ""),
        DelimitedBytes.split(StringUtils.toUtf8("a\tbc\t"), TSV, DelimitedBytes.UNKNOWN_FIELD_COUNT)
    );
  }

  @Test
  public void testMoreFieldsThanHint()
  {
    Assert.assertEquals(
        Arrays.asList("a", "b", "c"),
        DelimitedBytes.split(StringUtils.toUtf8("a\tb\tc"), TSV, 1)
    );
  }
}
