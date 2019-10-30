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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.storage.hdfs.HdfsStorageDruidModule;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class HdfsFirehoseFactoryTest
{
  @Test
  public void testArrayPaths() throws IOException
  {
    final HdfsFirehoseFactory firehoseFactory = new HdfsFirehoseFactory(
        null,
        Collections.singletonList("/foo/bar"),
        null,
        null,
        null,
        null,
        null
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
    final HdfsFirehoseFactory firehoseFactory = new HdfsFirehoseFactory(null, "/foo/bar", null, null, null, null, null);
    final ObjectMapper mapper = createMapper();

    final HdfsFirehoseFactory firehoseFactory2 = (HdfsFirehoseFactory)
        mapper.readValue(mapper.writeValueAsString(firehoseFactory), FirehoseFactory.class);

    Assert.assertEquals(
        firehoseFactory.getInputPaths(),
        firehoseFactory2.getInputPaths()
    );
  }

  private static ObjectMapper createMapper()
  {
    final ObjectMapper mapper = new ObjectMapper();
    new HdfsStorageDruidModule().getJacksonModules().forEach(mapper::registerModule);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(Configuration.class, new Configuration()));
    return mapper;
  }
}
