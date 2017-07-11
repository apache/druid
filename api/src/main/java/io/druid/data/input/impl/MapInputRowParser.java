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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class MapInputRowParser implements InputRowParser<Map<String, Object>>
{
  private final ParseSpec parseSpec;

  @JsonCreator
  public MapInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
  }

  @Override
  public InputRow parse(Map<String, Object> theMap)
  {
    final List<String> dimensions = parseSpec.getDimensionsSpec().hasCustomDimensions()
                                    ? parseSpec.getDimensionsSpec().getDimensionNames()
                                    : Lists.newArrayList(
                                        Sets.difference(
                                            theMap.keySet(),
                                            parseSpec.getDimensionsSpec()
                                                     .getDimensionExclusions()
                                        )
                                    );

    final DateTime timestamp;
    try {
      timestamp = parseSpec.getTimestampSpec().extractTimestamp(theMap);
      if (timestamp == null) {
        final String input = theMap.toString();
        throw new NullPointerException(
            StringUtils.format(
                "Null timestamp in input: %s",
                input.length() < 100 ? input : input.substring(0, 100) + "..."
            )
        );
      }
    }
    catch (Exception e) {
      throw new ParseException(e, "Unparseable timestamp found!");
    }

    return new MapBasedInputRow(timestamp.getMillis(), dimensions, theMap);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new MapInputRowParser(parseSpec);
  }
}
