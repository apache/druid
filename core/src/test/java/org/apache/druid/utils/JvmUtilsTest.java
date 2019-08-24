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

package org.apache.druid.utils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

public class JvmUtilsTest
{
  @Test
  public void testgetMaxDirectMemory()
  {
    try {
      long maxMemory = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
      Assert.assertTrue((maxMemory > 0));
    }
    catch (UnsupportedOperationException expected) {
      Assert.assertTrue(true);
    }
    catch (RuntimeException expected) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testSystemClassPath()
  {
    ClassLoader testClassLoader = this.getClass().getClassLoader();
    // ignore this test unless we can assume URLClassLoader (only applies to Java 8)
    Assume.assumeTrue(testClassLoader instanceof URLClassLoader);

    List<URL> parsedUrls = JvmUtils.systemClassPath();
    List<URL> classLoaderUrls = Arrays.asList(((URLClassLoader) testClassLoader).getURLs());

    Assert.assertEquals(classLoaderUrls, parsedUrls);
  }
}
