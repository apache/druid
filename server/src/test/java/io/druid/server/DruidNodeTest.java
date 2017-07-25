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

package io.druid.server;

import com.google.common.net.HostAndPort;
import io.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

public class DruidNodeTest
{
  private ServerConfig serverConfig;
  private Field plainTextField;
  private Field tlsField;

  @Before
  public void setUp() throws Exception
  {
    serverConfig = new ServerConfig();
    plainTextField = serverConfig.getClass().getDeclaredField("plaintext");
    tlsField = serverConfig.getClass().getDeclaredField("tls");
    plainTextField.setAccessible(true);
    tlsField.setAccessible(true);
  }

  @Test
  public void testDefaultsAndSanity() throws Exception
  {
    final String service = "test/service";

    DruidNode node;

    node = new DruidNode(service, null, null, null, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    // Hosts which report only ipv6 will have getDefaultHost() report something like fe80::6e40:8ff:fe93:9230
    // but getHostAndPort() reports [fe80::6e40:8ff:fe93:9230]
    Assert.assertEquals(HostAndPort.fromString(DruidNode.getDefaultHost()).toString(), node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", -1, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", null, null, serverConfig);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", null, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", null, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", null, null, serverConfig);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", 123, null, serverConfig);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", 123, null, serverConfig);
    Assert.assertEquals("abc", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]:123", null, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", 123, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", 123, null, serverConfig);
    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, null, 123, null, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());

    node = new DruidNode(service, null, 123, 123, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host", 123, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host:123", null, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", "host:123", null, 214, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(214, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", "host", 123, 214, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(214, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", "host:123", 123, 214, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(214, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", null, 123, 214, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(214, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", "host:123", null, 214, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(214, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test", "host:123", null, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(123, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test",null, null, 123, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(123, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test",null, -1, 123, serverConfig);
    Assert.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(123, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, true);
    node = new DruidNode("test","host", -1, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(123, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, false);
    node = new DruidNode("test","host", -1, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(-1, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, false);
    node = new DruidNode("test","host:123", 123, null, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());

    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, false);
    node = new DruidNode("test","host:123", null, 123, serverConfig);
    Assert.assertEquals("host", node.getHost());
    Assert.assertEquals(123, node.getPlaintextPort());
    Assert.assertEquals(-1, node.getTlsPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPorts() throws Exception
  {
    new DruidNode("test/service", "abc:123", 456, null, new ServerConfig());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAtLeastTlsOrPlainTextIsSet() throws Exception
  {
    plainTextField.setBoolean(serverConfig, false);
    tlsField.setBoolean(serverConfig, false);
    new DruidNode("test", "host:123", null, 123, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSamePlainTextAndTlsPort() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host:123", null, 123, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSamePlainTextAndTlsPort1() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host", 123, 123, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTlsPort() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host:123", null, null, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullPlainTextAndTlsPort1() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host", null, null, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTlsPort1() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host:123", 123, null, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullPlainTextAndTlsPort() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", null, null, null, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPlainTextPort() throws Exception
  {
    plainTextField.setBoolean(serverConfig, true);
    tlsField.setBoolean(serverConfig, true);
    new DruidNode("test", "host:123", 321, null, serverConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidIPv6WithPort() throws Exception
  {
    new DruidNode("test/service", "[abc:fff]:123", 456, null, new ServerConfig());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidIPv6() throws Exception
  {
    new DruidNode("test/service", "abc:fff", 456, null, new ServerConfig());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConflictingPortsNonsense() throws Exception
  {
    new DruidNode("test/service", "[2001:db8:85a3::8a2e:370:7334]:123", 456, null, new ServerConfig());
  }

  @Test
  public void testEquals() throws Exception
  {
    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    final ServerConfig serverConfig = new ServerConfig();
    Assert.assertEquals(new DruidNode(serviceName, host, port, null, serverConfig), new DruidNode(serviceName, host, port, null, serverConfig));
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig), new DruidNode(serviceName, host, -1, null, serverConfig));
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig), new DruidNode(serviceName, "other.host", port, null, serverConfig));
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig), new DruidNode("otherServiceName", host, port, null, serverConfig));
  }

  @Test
  public void testHashCode() throws Exception
  {

    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    final ServerConfig serverConfig = new ServerConfig();
    Assert.assertEquals(new DruidNode(serviceName, host, port, null, serverConfig).hashCode(), new DruidNode(serviceName, host, port, null, serverConfig).hashCode());
    // Potential hash collision if hashCode method ever changes
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig).hashCode(), new DruidNode(serviceName, host, -1, null, serverConfig).hashCode());
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig).hashCode(), new DruidNode(serviceName, "other.host", port, null, serverConfig).hashCode());
    Assert.assertNotEquals(new DruidNode(serviceName, host, port, null, serverConfig).hashCode(), new DruidNode("otherServiceName", host, port, null, serverConfig).hashCode());
  }
}
