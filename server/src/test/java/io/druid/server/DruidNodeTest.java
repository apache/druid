/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server;

import org.junit.Assert;
import org.junit.Test;

public class DruidNodeTest
{
  @Test
  public void testDefaultsAndSanity() throws Exception
  {
    final String service = "test/service";

    DruidNode node;

    node = new DruidNode(service, null, null);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(-1, node.getPort());
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", -1);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(-1, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", null);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", null);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertTrue(8080 <= node.getPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", null);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertTrue(8080 <= node.getPort());

    node = new DruidNode(service, "abc", null);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertTrue(8080 <= node.getPort());

    node = new DruidNode(service, "abc", 123);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", 123);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]:123", null);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", 123);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", 123);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, null, 123);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(123, node.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPorts() throws Exception
  {
    new DruidNode("test/service", "abc:123", 456);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidIPv6WithPort() throws Exception
  {
    new DruidNode("test/service", "[abc:fff]:123", 456);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidIPv6() throws Exception
  {
    new DruidNode("test/service", "abc:fff", 456);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPortsNonsense() throws Exception
  {
    new DruidNode("test/service", "[2001:db8:85a3::8a2e:370:7334]:123", 456);
  }
}
