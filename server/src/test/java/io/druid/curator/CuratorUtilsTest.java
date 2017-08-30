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

package io.druid.curator;

import io.druid.java.util.common.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CuratorUtilsTest extends CuratorTestBase
{
  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  @Test(timeout = 10_000L)
  public void testCreateIfNotExists() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    CuratorUtils.createIfNotExists(
        curator,
        "/foo/bar",
        CreateMode.PERSISTENT,
        StringUtils.toUtf8("baz"),
        CuratorUtils.DEFAULT_MAX_ZNODE_BYTES
    );
    Assert.assertEquals("baz", StringUtils.fromUtf8(curator.getData().forPath("/foo/bar")));

    CuratorUtils.createIfNotExists(
        curator,
        "/foo/bar",
        CreateMode.PERSISTENT,
        StringUtils.toUtf8("qux"),
        CuratorUtils.DEFAULT_MAX_ZNODE_BYTES
    );
    Assert.assertEquals("baz", StringUtils.fromUtf8(curator.getData().forPath("/foo/bar")));
  }

  @Test(timeout = 10_000L)
  public void testCreateIfNotExistsPayloadTooLarge() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    Exception thrown = null;
    try {
      CuratorUtils.createIfNotExists(
          curator,
          "/foo/bar",
          CreateMode.PERSISTENT,
          StringUtils.toUtf8("baz"),
          2
      );
    }
    catch (Exception e) {
      thrown = e;
    }

    Assert.assertTrue(thrown instanceof IllegalArgumentException);
    Assert.assertNull(curator.checkExists().forPath("/foo/bar"));
  }

  @Test(timeout = 10_000L)
  public void testCreateOrSet() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    Assert.assertNull(curator.checkExists().forPath("/foo/bar"));

    CuratorUtils.createOrSet(
        curator,
        "/foo/bar",
        CreateMode.PERSISTENT,
        StringUtils.toUtf8("baz"),
        3
    );
    Assert.assertEquals("baz", StringUtils.fromUtf8(curator.getData().forPath("/foo/bar")));

    CuratorUtils.createOrSet(
        curator,
        "/foo/bar",
        CreateMode.PERSISTENT,
        StringUtils.toUtf8("qux"),
        3
    );
    Assert.assertEquals("qux", StringUtils.fromUtf8(curator.getData().forPath("/foo/bar")));
  }

  @Test(timeout = 10_000L)
  public void testCreateOrSetPayloadTooLarge() throws Exception
  {
    curator.start();
    curator.blockUntilConnected();

    Exception thrown = null;
    try {
      CuratorUtils.createOrSet(
          curator,
          "/foo/bar",
          CreateMode.PERSISTENT,
          StringUtils.toUtf8("baz"),
          2
      );
    }
    catch (Exception e) {
      thrown = e;
    }

    Assert.assertTrue(thrown instanceof IllegalArgumentException);
    Assert.assertNull(curator.checkExists().forPath("/foo/bar"));
  }
}
