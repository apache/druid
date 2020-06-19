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

package org.apache.druid.emitter.graphite;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class GraphiteEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
                ObjectMapper.class,
                new DefaultObjectMapper()
            ));
  }

  @Test
  public void testSerDeserGraphiteEmitterConfig() throws IOException
  {
    GraphiteEmitterConfig graphiteEmitterConfig = new GraphiteEmitterConfig(
        "hostname",
        8080,
        1000,
        GraphiteEmitterConfig.PICKLE_PROTOCOL,
        1000L,
        100,
        new SendAllGraphiteEventConverter("prefix", true, true, false),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        null
    );
    String graphiteEmitterConfigString = mapper.writeValueAsString(graphiteEmitterConfig);
    GraphiteEmitterConfig graphiteEmitterConfigExpected = mapper.readerFor(GraphiteEmitterConfig.class).readValue(
        graphiteEmitterConfigString
    );
    Assert.assertEquals(graphiteEmitterConfigExpected, graphiteEmitterConfig);
  }

  @Test
  public void testSerDeserDruidToGraphiteEventConverter() throws IOException
  {
    SendAllGraphiteEventConverter sendAllGraphiteEventConverter = new SendAllGraphiteEventConverter(
        "prefix",
        true,
        true,
        false
    );
    String noopGraphiteEventConverterString = mapper.writeValueAsString(sendAllGraphiteEventConverter);
    DruidToGraphiteEventConverter druidToGraphiteEventConverter = mapper.readerFor(DruidToGraphiteEventConverter.class)
                                                                        .readValue(noopGraphiteEventConverterString);
    Assert.assertEquals(druidToGraphiteEventConverter, sendAllGraphiteEventConverter);

    WhiteListBasedConverter whiteListBasedConverter = new WhiteListBasedConverter(
        "prefix",
        true,
        true,
        false,
        "",
        new DefaultObjectMapper()
    );
    String whiteListBasedConverterString = mapper.writeValueAsString(whiteListBasedConverter);
    druidToGraphiteEventConverter = mapper.readerFor(DruidToGraphiteEventConverter.class)
                                          .readValue(whiteListBasedConverterString);
    Assert.assertEquals(druidToGraphiteEventConverter, whiteListBasedConverter);
  }

  @Test
  public void testJacksonModules()
  {
    Assert.assertTrue(new GraphiteEmitterModule().getJacksonModules().isEmpty());
  }
}
