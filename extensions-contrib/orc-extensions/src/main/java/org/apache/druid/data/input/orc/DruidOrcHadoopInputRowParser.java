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

package org.apache.druid.data.input.orc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DruidOrcHadoopInputRowParser implements InputRowParser<Map<String, Object>>
{
  private static final Logger log = new Logger(DruidOrcHadoopInputRowParser.class);

  private final ParseSpec parseSpec;
  private final List<String> dimensions;

  @JsonCreator
  public DruidOrcHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
  }

  @SuppressWarnings("ArgumentParameterSwap")
  @Override
  public List<InputRow> parseBatch(Map<String, Object> input)
  {
    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(input);
    return ImmutableList.of(new MapBasedInputRow(dateTime, dimensions, input));
  }

  @Override
  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new DruidOrcHadoopInputRowParser(parseSpec);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DruidOrcHadoopInputRowParser that = (DruidOrcHadoopInputRowParser) o;
    return Objects.equals(parseSpec, that.parseSpec);
  }

  @Override
  public int hashCode()
  {
    return parseSpec.hashCode();
  }

  @Override
  public String toString()
  {
    return "DruidOrcHadoopInputRowParser{" +
           "parseSpec=" + parseSpec +
           '}';
  }
}
