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

package org.apache.druid.indexer.hbase.util;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HBaseUtilTest
{

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception
  {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception
  {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception
  {
  }

  @Test
  public void testRemoveConflictingLibrary()
  {
    Stream<URL> urlStream = Arrays.stream("/aaa-1.1.1.jar:/bbb-2.2.2.jar:/ccc-3.3.3.jar".split(":")).map(jar -> {
      try {
        return new URL("test", "test", 10000, jar, new URLStreamHandler()
        {
          @Override
          protected URLConnection openConnection(URL u) throws IOException
          {
            return null;
          }

        });
      }
      catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    });

    Map<String, String> conflictLibMap = new HashMap<>();
    conflictLibMap.put("bbb", "2.2.2");

    List<URL> resultList = HBaseUtil.removeConflictingLibrary(urlStream, conflictLibMap);
    Assert.assertThat(false, CoreMatchers.is(resultList.stream().anyMatch(u -> {
      return u.getFile().contains("bbb");
    })));
  }

}
