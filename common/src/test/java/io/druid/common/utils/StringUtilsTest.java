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

import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 */
public class StringUtilsTest
{
  // copied from https://github.com/druid-io/druid/pull/2612
  public final static String[] TEST_STRINGS = new String[]{
      "peach", "péché", "pêche", "sin", "",
      "☃", "C", "c", "Ç", "ç", "G", "g", "Ğ", "ğ", "I", "ı", "İ", "i",
      "O", "o", "Ö", "ö", "S", "s", "Ş", "ş", "U", "u", "Ü", "ü", "ä",
      "\uD841\uDF0E",
      "\uD841\uDF31",
      "\uD844\uDC5C",
      "\uD84F\uDCB7",
      "\uD860\uDEE2",
      "\uD867\uDD98",
      "\u006E\u0303",
      "\u006E",
      "\uFB00",
      "\u0066\u0066",
      "Å",
      "\u00C5",
      "\u212B"
  };

  @Test
  public void binaryLengthAsUTF8Test() throws UnsupportedEncodingException
  {
    for (String string : TEST_STRINGS) {
      Assert.assertEquals(StringUtils.toUtf8(string).length, StringUtils.estimatedBinaryLengthAsUTF8(string));
    }
  }

  @Test
  public void binaryLengthAsUTF8InvalidTest() throws UnsupportedEncodingException
  {
    // we can fix this but looks trivial case, imho
    String invalid = "\uD841";  // high only
    Assert.assertEquals(1, StringUtils.toUtf8(invalid).length);
    Assert.assertEquals(4, StringUtils.estimatedBinaryLengthAsUTF8(invalid));

    invalid = "\uD841\uD841";  // high + high
    Assert.assertEquals(2, StringUtils.toUtf8(invalid).length);
    Assert.assertEquals(4, StringUtils.estimatedBinaryLengthAsUTF8(invalid));

    invalid = "\uD841\u0050";  // high + char
    Assert.assertEquals(2, StringUtils.toUtf8(invalid).length);
    Assert.assertEquals(4, StringUtils.estimatedBinaryLengthAsUTF8(invalid));

    invalid = "\uDEE2\uD841";  // low + high
    Assert.assertEquals(2, StringUtils.toUtf8(invalid).length);
    Assert.assertEquals(4, StringUtils.estimatedBinaryLengthAsUTF8(invalid));
  }

}
