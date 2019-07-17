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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class InfluxdbEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();
  private InfluxdbEmitterConfig influxdbEmitterConfig;

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        ObjectMapper.class,
        new DefaultObjectMapper()
    ));

    influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
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
  public void testInfluxdbEmitterConfigObjectsAreDifferent() throws IOException
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8080,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    Assert.assertNotEquals(influxdbEmitterConfig, influxdbEmitterConfigComparison);
  }

  @Test(expected = NullPointerException.class)
  public void testConfigWithNullHostname() throws IOException
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
        null,
        8080,
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
  public void testConfigWithNullPort() throws IOException
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullPort = new InfluxdbEmitterConfig(
        "localhost",
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
    Assert.assertEquals(expectedPort, influxdbEmitterConfig.getPort());
  }

  @Test
  public void testEqualsMethod()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    Assert.assertTrue(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
  }

  @Test
  public void testEqualsMethodWithNotEqualConfigs()
  {
    InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        10000,
        "adam",
        "password",
        null
    );
    Assert.assertFalse(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
  }

  @Test(expected = NullPointerException.class)
  public void testConfigWithNullInfluxdbUserName() throws IOException
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        null,
        "password",
        null
    );
  }

  @Test(expected = NullPointerException.class)
  public void testConfigWithNullInfluxdbPassword() throws IOException
  {
    InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        null,
        null
    );
  }

  @Test
  public void testConfigWithNullDimensionWhitelist()
  {
    InfluxdbEmitterConfig influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    ImmutableSet<String> expected = ImmutableSet.copyOf(Arrays.asList("dataSource", "type", "numMetrics", "numDimensions", "threshold", "dimension", "taskType", "taskStatus", "tier"));
    Assert.assertEquals(expected, influxdbEmitterConfig.getDimensionWhitelist());
  }

  @Test
  public void testConfigWithDimensionWhitelist()
  {
    InfluxdbEmitterConfig influxdbEmitterConfig = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        ImmutableSet.of("dataSource", "taskType")
    );
    ImmutableSet<String> expected = ImmutableSet.copyOf(Arrays.asList("dataSource", "taskType"));
    Assert.assertEquals(expected, influxdbEmitterConfig.getDimensionWhitelist());
  }

}
