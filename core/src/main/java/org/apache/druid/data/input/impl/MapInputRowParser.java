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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapInputRowParser implements InputRowParser<Map<String, Object>>
{
  private final ParseSpec parseSpec;
  private final List<String> dimensions;

  @JsonCreator
  public MapInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
  }

  @Override
  public List<InputRow> parseBatch(Map<String, Object> theMap)
  {
    return ImmutableList.of(
        parse(
            parseSpec.getTimestampSpec(),
            dimensions,
            parseSpec.getDimensionsSpec().getDimensionExclusions(),
            theMap
        )
    );
  }

  public static InputRow parse(InputRowSchema inputRowSchema, Map<String, Object> theMap) throws ParseException
  {
    return parse(inputRowSchema.getTimestampSpec(), inputRowSchema.getDimensionsSpec(), theMap);
  }

  public static InputRow parse(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      Map<String, Object> theMap
  ) throws ParseException
  {
    return parse(timestampSpec, dimensionsSpec.getDimensionNames(), dimensionsSpec.getDimensionExclusions(), theMap);
  }

  public static InputRow parse(
      TimestampSpec timestampSpec,
      List<String> dimensions,
      Set<String> dimensionExclusions,
      Map<String, Object> theMap
  ) throws ParseException
  {
    final List<String> dimensionsToUse;
    if (!dimensions.isEmpty()) {
      dimensionsToUse = dimensions;
    } else {
      dimensionsToUse = new ArrayList<>(Sets.difference(theMap.keySet(), dimensionExclusions));
    }

    final DateTime timestamp;
    try {
      timestamp = timestampSpec.extractTimestamp(theMap);
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
      throw new ParseException(e, "Unparseable timestamp found! Event: %s", theMap);
    }

    return new MapBasedInputRow(timestamp, dimensionsToUse, theMap);
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
