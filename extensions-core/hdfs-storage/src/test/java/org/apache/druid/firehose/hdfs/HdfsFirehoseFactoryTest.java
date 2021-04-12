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

package org.apache.druid.firehose.hdfs;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.inputsource.hdfs.HdfsInputSourceConfig;
import org.apache.druid.storage.hdfs.HdfsStorageDruidModule;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class HdfsFirehoseFactoryTest
{
  private static final HdfsInputSourceConfig DEFAULT_INPUT_SOURCE_CONFIG = new HdfsInputSourceConfig(null);
  private static final Configuration DEFAULT_CONFIGURATION = new Configuration();

  @BeforeClass
  public static void setup()
  {
    DEFAULT_CONFIGURATION.set("fs.default.name", "hdfs://localhost:7020");
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testArrayPaths() throws IOException
  {
    final HdfsFirehoseFactory firehoseFactory = new HdfsFirehoseFactory(
        Collections.singletonList("/foo/bar"),
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );

    final ObjectMapper mapper = createMapper();

    final HdfsFirehoseFactory firehoseFactory2 = (HdfsFirehoseFactory)
        mapper.readValue(mapper.writeValueAsString(firehoseFactory), FirehoseFactory.class);

    Assert.assertEquals(
        firehoseFactory.getInputPaths(),
        firehoseFactory2.getInputPaths()
    );
  }

  @Test
  public void testStringPaths() throws IOException
  {
    final HdfsFirehoseFactory firehoseFactory = new HdfsFirehoseFactory(
        "/foo/bar",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );
    final ObjectMapper mapper = createMapper();

    final HdfsFirehoseFactory firehoseFactory2 = (HdfsFirehoseFactory)
        mapper.readValue(mapper.writeValueAsString(firehoseFactory), FirehoseFactory.class);

    Assert.assertEquals(
        firehoseFactory.getInputPaths(),
        firehoseFactory2.getInputPaths()
    );
  }

  @Test
  public void testConstructorAllowsOnlyDefaultProtocol()
  {
    new HdfsFirehoseFactory(
        "hdfs://localhost:7020/foo/bar",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only [hdfs] protocols are allowed");
    new HdfsFirehoseFactory(
        "file:/foo/bar",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );
  }

  @Test
  public void testConstructorAllowsOnlyCustomProtocol()
  {
    final Configuration conf = new Configuration();
    conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem");
    new HdfsFirehoseFactory(
        "ftp://localhost:21/foo/bar",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        new HdfsInputSourceConfig(ImmutableSet.of("ftp"))
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Only [druid] protocols are allowed");
    new HdfsFirehoseFactory(
        "hdfs://localhost:7020/foo/bar",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        new HdfsInputSourceConfig(ImmutableSet.of("druid"))
    );
  }

  @Test
  public void testConstructorWithDefaultHdfs()
  {
    new HdfsFirehoseFactory(
        "/foo/bar*",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );

    new HdfsFirehoseFactory(
        "foo/bar*",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );

    new HdfsFirehoseFactory(
        "hdfs:///foo/bar*",
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );

    new HdfsFirehoseFactory(
        "hdfs://localhost:10020/foo/bar*", // different hdfs
        null,
        null,
        null,
        null,
        null,
        DEFAULT_CONFIGURATION,
        DEFAULT_INPUT_SOURCE_CONFIG
    );
  }

  private static ObjectMapper createMapper()
  {
    final ObjectMapper mapper = new ObjectMapper();
    new HdfsStorageDruidModule().getJacksonModules().forEach(mapper::registerModule);
    mapper.setInjectableValues(
        new Std()
            .addValue(Configuration.class, DEFAULT_CONFIGURATION)
            .addValue(HdfsInputSourceConfig.class, DEFAULT_INPUT_SOURCE_CONFIG)
    );
    return mapper;
  }
}
