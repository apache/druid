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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;

public class MapWritableInputRowParser implements InputRowParser<MapWritable>
{
  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;

  @JsonCreator
  public MapWritableInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
  }

  @Override
  public InputRow parse(MapWritable map)
  {
    return mapParser.parse(convertMapStringAndObject(map));
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new MapWritableInputRowParser(parseSpec);
  }

  private Map<String, Object> convertMapStringAndObject(MapWritable value)
  {
    Map<String, Object> map = new HashMap<String, Object>(value.size());
    for(Map.Entry<Writable, Writable> e : value.entrySet()) {
      if(! (e.getKey() instanceof Text)) {
        throw new RuntimeException("Found non-Text key in input record. type: " + e.getKey().getClass().getName());
      }

      if(e.getValue() instanceof IntWritable) {
        map.put(e.getKey().toString(), ((IntWritable)e.getValue()).get());
      } else if(e.getValue() instanceof LongWritable) {
        map.put(e.getKey().toString(), ((LongWritable)e.getValue()).get());
      } else if(e.getValue() instanceof FloatWritable) {
        map.put(e.getKey().toString(), ((FloatWritable)e.getValue()).get());
      } else if(e.getValue() instanceof DoubleWritable) {
        map.put(e.getKey().toString(), ((DoubleWritable)e.getValue()).get());
      } else if(e.getValue() instanceof Text) {
        map.put(e.getKey().toString(), e.getValue().toString());
      } else if(e.getValue() instanceof BytesWritable) {
        map.put(e.getKey().toString(), ((BytesWritable)e.getValue()).getBytes());
      } else if(e.getValue() instanceof ArrayWritable) {
        //this is for multivalued dimensions
        map.put(
            e.getKey().toString(),
            Lists.transform(Arrays.asList(((ArrayWritable) e.getValue()).get()), new Function<Writable, String>()
            {
              @Override
              public String apply(Writable input)
              {
                return ((Text) input).toString();
              }
            }));
      } else {
        throw new RuntimeException("Unrecognized value type in input record. type: " + e.getValue().getClass().getName());
      }
    }
    return map;
  }
}
