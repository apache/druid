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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.IAE;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

/**
 */
public class HadoopyStringInputRowParser implements InputRowParser<Object>
{
  private final StringInputRowParser parser;

  public HadoopyStringInputRowParser(@JsonProperty("parseSpec") ParseSpec parseSpec)
  {
    this.parser = new StringInputRowParser(parseSpec, null);
  }

  @Override
  public InputRow parse(Object input)
  {
    if (input instanceof Text) {
      return parser.parse(((Text) input).toString());
    } else if (input instanceof BytesWritable) {
      BytesWritable valueBytes = (BytesWritable) input;
      return parser.parse(ByteBuffer.wrap(valueBytes.getBytes(), 0, valueBytes.getLength()));
    } else {
      throw new IAE("can't convert type [%s] to InputRow", input.getClass().getName());
    }
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parser.getParseSpec();
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new HadoopyStringInputRowParser(parseSpec);
  }
}
