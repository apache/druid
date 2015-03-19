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

package io.druid.indexer.parser;

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.initialization.IndexingHadoopDruidModule;
import io.druid.jackson.DefaultObjectMapper;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class MapWritableInputRowParserTest
{
  private final ObjectMapper jsonMapper;

  public MapWritableInputRowParserTest()
  {
    jsonMapper = new DefaultObjectMapper();
    for (Module jacksonModule : new IndexingHadoopDruidModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testDeserialization() throws Exception
  {
    String str = "{" +
        "\"type\": \"mapWritableParser\",\n" +
        "\"parseSpec\": {\n" +
        "  \"format\": \"json\",\n" + //NOTE: may be druid-api should allow another name for json parseSpec
        "  \"timestampSpec\": { \"column\": \"time\", \"format\": \"YYYY\" },\n" +
        "  \"dimensionsSpec\": {}\n" +
        "  }\n" +
        "}";

    MapWritableInputRowParser parser = (MapWritableInputRowParser)jsonMapper.readValue(str, InputRowParser.class);
  }

  @Test
  public void testParse()
  {
    MapWritableInputRowParser parser = new MapWritableInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("time", "YYYY"),
            new DimensionsSpec(null, null, null)));
    
    MapWritable mapWritable = new MapWritable();
    mapWritable.put(new Text("time"), new Text("2015"));
    mapWritable.put(new Text("int"), new IntWritable(1));
    mapWritable.put(new Text("long"), new LongWritable(1));
    mapWritable.put(new Text("float"), new FloatWritable(1.0f));
    mapWritable.put(new Text("double"), new DoubleWritable(1.0));
    mapWritable.put(new Text("text"), new Text("a"));
    mapWritable.put(
        new Text("list"),
        new ArrayWritable(Text.class, new Text[]{ new Text("v1"), new Text("v2") }));
    
    byte[] bytes = "a".getBytes();
    mapWritable.put(new Text("bytes"), new BytesWritable(bytes));

    InputRow inputRow = parser.parse(mapWritable);
    
    Assert.assertEquals(DateTime.parse("2015"), inputRow.getTimestamp());
    Assert.assertEquals(1, inputRow.getLongMetric("int"));
    Assert.assertEquals(1, inputRow.getLongMetric("long"));
    Assert.assertEquals(1.0, inputRow.getFloatMetric("float"), 0.0001);
    Assert.assertEquals(1.0, inputRow.getFloatMetric("double"), 0.0001);
    Assert.assertEquals(ImmutableList.of("a"), inputRow.getDimension("text"));
    Assert.assertEquals(ImmutableList.of("v1", "v2"), inputRow.getDimension("list"));
    Assert.assertEquals(bytes, inputRow.getRaw("bytes"));
  }
}
