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

package io.druid.collections;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class CountingMapTest
{
  private CountingMap mapObject = null ;

  @Before
  public void setUp()
  {
    mapObject = new CountingMap();
  }

  @After
  public void tearDown()
  {
    mapObject.clear();
  }

  @Test
  public void testAdd()
  {
    long defaultValue = 10;
    String defaultKey = "defaultKey";
    long actual;
    actual = mapObject.add(defaultKey,defaultValue);
    Assert.assertEquals("Values does not match", actual, defaultValue);
  }

  @Test
  public void testSnapshot()
  {
    long defaultValue = 10;
    String defaultKey = "defaultKey";
    mapObject.add(defaultKey, defaultValue);
    ImmutableMap snapShotMap = (ImmutableMap) mapObject.snapshot();
    Assert.assertEquals("Maps size does not match",mapObject.size(),snapShotMap.size());
    long expected = (long) snapShotMap.get(defaultKey);
    AtomicLong actual = (AtomicLong) mapObject.get(defaultKey);
    Assert.assertEquals("Values for key = " + defaultKey + " does not match",
        actual.longValue(),expected);
  }
}
