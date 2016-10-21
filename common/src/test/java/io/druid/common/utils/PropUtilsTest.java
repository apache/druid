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

import org.junit.Assert;
import org.junit.Test;

import io.druid.java.util.common.ISE;

import java.util.Properties;

public class PropUtilsTest
{
  @Test(expected = ISE.class)
  public void testNotSpecifiedGetProperty()
  {
    Properties prop = new Properties();
    PropUtils.getProperty(prop,"");
  }

  @Test
  public void testGetProperty()
  {
    Properties prop = new Properties();
    prop.setProperty("key","value");
    Assert.assertEquals("value", PropUtils.getProperty(prop,"key"));
  }

  @Test(expected = ISE.class)
  public void testNotSpecifiedGetPropertyAsInt()
  {
    Properties prop = new Properties();
    PropUtils.getPropertyAsInt(prop,"",null);
  }

  @Test
  public void testDefaultValueGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int defaultValue = 1;
    int result = PropUtils.getPropertyAsInt(prop,"",defaultValue);
    Assert.assertEquals(defaultValue, result);
  }

  @Test
  public void testParseGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int expectedValue = 1;
    prop.setProperty("key", Integer.toString(expectedValue));
    int result = PropUtils.getPropertyAsInt(prop,"key");
    Assert.assertEquals(expectedValue, result);
  }

  @Test(expected = ISE.class)
  public void testFormatExceptionGetPropertyAsInt()
  {
    Properties prop = new Properties();
    prop.setProperty("key","1-value");
    PropUtils.getPropertyAsInt(prop,"key",null);
  }
}
