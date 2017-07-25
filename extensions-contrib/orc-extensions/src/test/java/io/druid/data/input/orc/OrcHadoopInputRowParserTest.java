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

package io.druid.data.input.orc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
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
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
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

  @Test
  public void testParse()
  {
    final String typeString = "struct<timestamp:string,col1:string,col2:array<string>,col3:float,col4:bigint,col5:decimal,col6:array<string>>";
    final OrcHadoopInputRowParser parser = new OrcHadoopInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec("timestamp", "auto", null),
            new DimensionsSpec(null, null, null)
        ),
        typeString
    );

    final SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(
        TypeInfoUtils.getTypeInfoFromTypeString(typeString)
    );
    final OrcStruct struct = (OrcStruct) oi.create();
    struct.setNumFields(7);
    oi.setStructFieldData(struct, oi.getStructFieldRef("timestamp"), new Text("2000-01-01"));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col1"), new Text("foo"));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col2"), ImmutableList.of(new Text("foo"), new Text("bar")));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col3"), new FloatWritable(1));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col4"), new LongWritable(2));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col5"), new HiveDecimalWritable(3));
    oi.setStructFieldData(struct, oi.getStructFieldRef("col6"), null);

    final InputRow row = parser.parse(struct);
    Assert.assertEquals("timestamp", new DateTime("2000-01-01"), row.getTimestamp());
    Assert.assertEquals("col1", "foo", row.getRaw("col1"));
    Assert.assertEquals("col2", ImmutableList.of("foo", "bar"), row.getRaw("col2"));
    Assert.assertEquals("col3", 1.0f, row.getRaw("col3"));
    Assert.assertEquals("col4", 2L, row.getRaw("col4"));
    Assert.assertEquals("col5", 3.0d, row.getRaw("col5"));
    Assert.assertNull("col6", row.getRaw("col6"));
  }
}
