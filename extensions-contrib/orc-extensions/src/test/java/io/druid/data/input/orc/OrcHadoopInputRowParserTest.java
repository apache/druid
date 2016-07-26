/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.data.input.orc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.data.input.impl.*;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class OrcHadoopInputRowParserTest
{
  Injector injector;
  ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    injector =  Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            },
            new OrcExtensionsModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
  }

  @Test
  public void testSerde() throws IOException
  {
    String parserString = "{\n" +
        "        \"type\": \"orc\",\n" +
        "        \"parseSpec\": {\n" +
        "          \"format\": \"timeAndDims\",\n" +
        "          \"timestampSpec\": {\n" +
        "            \"column\": \"timestamp\",\n" +
        "            \"format\": \"auto\"\n" +
        "          },\n" +
        "          \"dimensionsSpec\": {\n" +
        "            \"dimensions\": [\n" +
        "              \"col1\",\n" +
        "              \"col2\"\n" +
        "            ],\n" +
        "            \"dimensionExclusions\": [],\n" +
        "            \"spatialDimensions\": []\n" +
        "          }\n" +
        "        },\n" +
        "        \"typeString\": \"struct<timestamp:string,col1:string,col2:array<string>,val1:float>\"\n" +
        "      }";

    InputRowParser parser = mapper.readValue(parserString, InputRowParser.class);
    InputRowParser expected = new OrcHadoopInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec(
                "timestamp",
                "auto",
                null
            ),
            new DimensionsSpec(
                ImmutableList.<DimensionSchema>of(new StringDimensionSchema("col1"), new StringDimensionSchema("col2")),
                null,
                null
            )
        ),
        "struct<timestamp:string,col1:string,col2:array<string>,val1:float>"
    );

    Assert.assertEquals(expected, parser);
  }

  @Test
  public void testTypeFromParseSpec()
  {
    ParseSpec parseSpec = new TimeAndDimsParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            ImmutableList.<DimensionSchema>of(new StringDimensionSchema("col1"), new StringDimensionSchema("col2")),
            null,
            null
        )
    );
    String typeString = OrcHadoopInputRowParser.typeStringFromParseSpec(parseSpec);
    String expected = "struct<timestamp:string,col1:string,col2:string>";

    Assert.assertEquals(expected, typeString);
  }

  
}
