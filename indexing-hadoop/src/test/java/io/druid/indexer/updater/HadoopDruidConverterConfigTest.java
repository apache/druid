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

package io.druid.indexer.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;

public class HadoopDruidConverterConfigTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void simpleSerDe() throws IOException
  {
    final HadoopDruidConverterConfig config = new HadoopDruidConverterConfig(
        "datasource",
        Interval.parse("2000/2010"),
        new IndexSpec(),
        ImmutableList.<DataSegment>of(),
        true,
        URI.create("file:/dev/null"),
        ImmutableMap.<String, String>of(),
        "HIGH",
        temporaryFolder.newFolder().getAbsolutePath()
    );
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(HadoopDruidConverterConfig.class);
    final byte[] value = mapper.writeValueAsBytes(config);
    final HadoopDruidConverterConfig config2 = mapper.readValue(
        value,
        HadoopDruidConverterConfig.class
    );
    Assert.assertEquals(mapper.writeValueAsString(config), mapper.writeValueAsString(config2));
  }
}
