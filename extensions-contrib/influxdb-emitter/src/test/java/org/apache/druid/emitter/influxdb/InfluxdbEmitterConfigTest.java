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

package org.apache.druid.emitter.influxdb;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyStore;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class InfluxdbEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();
  private InfluxdbEmitterConfig influxdbEmitterConfig;

  @BeforeEach
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        ObjectMapper.class,
        new DefaultObjectMapper()
    ));

    influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
  }

  @Test
  public void testInfluxdbEmitterConfigObjectsAreDifferent()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8080,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    Assertions.assertNotEquals(influxdbEmitterConfig, influxdbEmitterConfigComparison);
  }

  @Test
  public void testConfigWithNullHostname()
  {
    assertThrows(NullPointerException.class, () -> {
      InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
          null,
          8080,
          null,
          null,
          null,
          null,
          "dbname",
          10000,
          15000,
          30000,
          "adam",
          "password",
          null
      );
    });
  }

  @Test
  public void testConfigWithNullPort()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullPort = new InfluxdbEmitterConfig(
        "localhost",
        null,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    int expectedPort = 8086;
    Assertions.assertEquals(expectedPort, influxdbEmitterConfig.getPort());
  }

  @Test
  public void testEqualsMethod()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    Assertions.assertTrue(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
  }

  @Test
  public void testEqualsMethodWithNotEqualConfigs()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "https",
        "/path",
        "jks",
        "password",
        "dbname",
        10000,
        15000,
        10000,
        "adam",
        "password",
        null
    );
    Assertions.assertFalse(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
  }

  @Test
  public void testConfigWithNullInfluxdbUserName()
  {
    assertThrows(NullPointerException.class, () -> {
      InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
          "localhost",
          8086,
          null,
          null,
          null,
          null,
          "dbname",
          10000,
          15000,
          30000,
          null,
          "password",
          null
      );
    });
  }

  @Test
  public void testConfigWithNullInfluxdbPassword()
  {
    assertThrows(NullPointerException.class, () -> {
      InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
          "localhost",
          8086,
          null,
          null,
          null,
          null,
          "dbname",
          10000,
          15000,
          30000,
          "adam",
          null,
          null
      );
    });
  }

  @Test
  public void testConfigWithNullDimensionWhitelist()
  {
    InfluxdbEmitterConfig influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    ImmutableSet<String> expected = ImmutableSet.copyOf(Arrays.asList("dataSource", "type", "numMetrics", "numDimensions", "threshold", "dimension", "taskType", "taskStatus", "tier"));
    Assertions.assertEquals(expected, influxdbEmitterConfig.getDimensionWhitelist());
  }

  @Test
  public void testConfigWithDimensionWhitelist()
  {
    InfluxdbEmitterConfig influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        null,
        null,
        null,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        ImmutableSet.of("dataSource", "taskType")
    );
    ImmutableSet<String> expected = ImmutableSet.copyOf(Arrays.asList("dataSource", "taskType"));
    Assertions.assertEquals(expected, influxdbEmitterConfig.getDimensionWhitelist());
  }

  @Test
  public void testConfigWithNullProtocol()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullProtocol = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        "path",
        "jks",
        "pass",
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    String expectedProtocol = "http";
    Assertions.assertEquals(expectedProtocol, influxdbEmitterConfigWithNullProtocol.getProtocol());
  }

  @Test
  public void testConfigEquals()
  {
    EqualsVerifier.forClass(InfluxdbEmitterConfig.class).withNonnullFields(
        "hostname", "port", "protocol", "trustStoreType", "databaseName",
        "maxQueueSize", "flushPeriod", "flushDelay", "influxdbUserName",
        "influxdbPassword", "dimensionWhitelist"
    ).withPrefabValues(
        com.google.common.collect.ImmutableSet.class,
        com.google.common.collect.ImmutableSet.of("a"),
        com.google.common.collect.ImmutableSet.of("b")
    ).usingGetClass().verify();
  }

  @Test
  public void testConfigWithNullTrustStoreType()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullTrustStoreType = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        null,
        "path",
        null,
        "pass",
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    String expectedTrustStoreType = KeyStore.getDefaultType();
    Assertions.assertEquals(expectedTrustStoreType, influxdbEmitterConfigWithNullTrustStoreType.getTrustStoreType());
  }

}
