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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;


public class DruidNodeTest
{
  private final ObjectMapper mapper;

  public DruidNodeTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(Integer.class, Integer.MAX_VALUE)
            .addValue(String.class, "DUMMY")
    );
  }

  @Test
  public void testDefaultsAndSanity()
  {
    final String service = "test/service";

    DruidNode node;

    node = new DruidNode(service, null, false, null, null, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    // Hosts which report only ipv6 will have getDefaultHost() report something like fe80::6e40:8ff:fe93:9230
    // but getHostAndPort() reports [fe80::6e40:8ff:fe93:9230]
    Assertions.assertEquals(HostAndPort.fromString(DruidNode.getDefaultHost()).toString(), node.getHostAndPort());
    Assertions.assertEquals(DruidNode.UNKNOWN_VERSION, node.getVersion()); // unknown because not compiled with version

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, -1, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", false, null, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", false, null, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", false, 123, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", false, 123, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]:123", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, 123, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", false, 123, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, null, false, 123, null, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());

    node = new DruidNode(service, null, false, 123, 123, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host", false, 123, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host:123", false, null, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host", false, 123, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, 123, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", null, false, 123, 214, true, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 214, false, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 123, false, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", null, false, null, 123, false, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", null, false, -1, 123, false, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", "host", false, -1, null, 123, false, true, ImmutableMap.of("labelKey1", "labelValue1"));
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());
    Assertions.assertEquals(ImmutableMap.of("labelKey1", "labelValue1"), node.getLabels());

    node = new DruidNode("test", "host", false, -1, null, 123, true, false, ImmutableMap.of("labelKey1", "labelValue1", "labelKey2", "labelValue2"));
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());
    Assertions.assertEquals(ImmutableMap.of("labelKey1", "labelValue1", "labelKey2", "labelValue2"), node.getLabels());

    node = new DruidNode("test", "host:123", false, 123, null, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());
    Assertions.assertNull(node.getLabels());

    node = new DruidNode("test", "host:123", false, null, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());
  }

  @Test
  public void testConflictingPorts()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "abc:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testAtLeastTlsOrPlainTextIsSet()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, 123, false, false);
    });
  }

  @Test
  public void testSamePlainTextAndTlsPort()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, 123, true, true);
    });
  }

  @Test
  public void testSamePlainTextAndTlsPort1()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host", false, 123, 123, true, true);
    });
  }

  @Test
  public void testNullTlsPort()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, null, true, true);
    });
  }

  @Test
  public void testNullPlainTextAndTlsPort1()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host", false, null, null, true, true);
    });
  }

  @Test
  public void testNullTlsPort1()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, 123, null, true, true);
    });
  }

  @Test
  public void testNullPlainTextAndTlsPort()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", null, false, null, null, true, true);
    });
  }

  @Test
  public void testConflictingPlainTextPort()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, 321, null, true, true);
    });
  }

  @Test
  public void testInvalidIPv6WithPort()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "[abc:fff]:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testInvalidIPv6()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "abc:fff", false, 456, null, true, false);
    });
  }

  @Test
  public void testConflictingPortsNonsense()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "[2001:db8:85a3::8a2e:370:7334]:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testEquals()
  {
    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    final Map<String, String> labels = ImmutableMap.of("key1", "value1");
    Assertions.assertEquals(new DruidNode(serviceName, host, false, port, null, null, true, false, labels), new DruidNode(serviceName, host, false, port, null, null, true, false, labels));
    Assertions.assertEquals(new DruidNode(serviceName, host, false, port, null, null, true, false, labels), new DruidNode(serviceName, host, false, port, null, null, true, false, ImmutableMap.of("key1", "value1")));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode(serviceName, host, false, -1, null, true, false));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode(serviceName, "other.host", false, port, null, true, false));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode("otherServiceName", host, false, port, null, true, false));
  }

  @Test
  public void testHashCode()
  {

    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    final Map<String, String> labels = ImmutableMap.of("key1", "value1");
    Assertions.assertEquals(
        new DruidNode(serviceName, host, false, port, null, null, true, false, labels).hashCode(),
        new DruidNode(serviceName, host, false, port, null, null, true, false, labels).hashCode()
    );
    // Potential hash collision if hashCode method ever changes
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode(serviceName, host, false, -1, null, true, false).hashCode());
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode(serviceName, "other.host", false, port, null, true, false).hashCode());
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode("otherServiceName", host, false, port, null, true, false).hashCode());
  }


  @Test
  public void testSerde1() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", true, 1234, null, 5678, true, true, ImmutableMap.of("key1", "value1"))),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertTrue(actual.isBindOnHost());
    Assertions.assertTrue(actual.isEnablePlaintextPort());
    Assertions.assertTrue(actual.isEnableTlsPort());
    Assertions.assertEquals(1234, actual.getPlaintextPort());
    Assertions.assertEquals(5678, actual.getTlsPort());
    Assertions.assertEquals(ImmutableMap.of("key1", "value1"), actual.getLabels());
  }

  @Test
  public void testSerde2() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", false, 1234, null, 5678, null, false, null)),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertFalse(actual.isBindOnHost());
    Assertions.assertTrue(actual.isEnablePlaintextPort());
    Assertions.assertFalse(actual.isEnableTlsPort());
    Assertions.assertEquals(1234, actual.getPlaintextPort());
    Assertions.assertEquals(-1, actual.getTlsPort());
    Assertions.assertNull(actual.getLabels());
  }

  @Test
  public void testSerde3() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", true, 1234, null, 5678, false, true, ImmutableMap.of("key1", "value1", "key2", "value2"))),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertTrue(actual.isBindOnHost());
    Assertions.assertFalse(actual.isEnablePlaintextPort());
    Assertions.assertTrue(actual.isEnableTlsPort());
    Assertions.assertEquals(-1, actual.getPlaintextPort());
    Assertions.assertEquals(5678, actual.getTlsPort());
    Assertions.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), actual.getLabels());
  }

  @Test
  public void testDeserialization1() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"bindOnHost\":true,\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678,\n"
                  + "  \"enablePlaintextPort\":true,\n"
                  + "  \"enableTlsPort\":true,\n"
                  + "  \"labels\":{\"key1\":\"value1\"}"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", true, 1234, null, 5678, true, true, ImmutableMap.of("key1", "value1")), actual);

    Assertions.assertEquals("https", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertEquals("host:5678", actual.getHostAndTlsPort());
    Assertions.assertEquals("host:5678", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization2() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678,\n"
                  + "  \"enablePlaintextPort\":true"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, 1234, null, 5678, true, false, null), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization3() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, 1234, null, 5678, null, false, null), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization4() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"port\":1234,\n"
                  + "  \"tlsPort\":5678"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, null, 1234, 5678, null, false, null), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

}
