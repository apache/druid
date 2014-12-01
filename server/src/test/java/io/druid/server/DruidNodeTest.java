/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
    Assert.assertEquals(DruidNode.DEFAULT_HOST, node.getHost());
    Assert.assertEquals(-1, node.getPort());

    node = new DruidNode(service, "abc:123", null);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", null);
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHost());
    Assert.assertTrue(8080 <= node.getPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", null);
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHost());
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
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", 123);
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", 123);
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHost());
    Assert.assertEquals(123, node.getPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, null, 123);
    Assert.assertEquals(DruidNode.DEFAULT_HOST, node.getHost());
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
