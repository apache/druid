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
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
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

  private static InputRow parse(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      Map<String, Object> theMap
  ) throws ParseException
  {
    return parse(timestampSpec, dimensionsSpec.getDimensionNames(), dimensionsSpec.getDimensionExclusions(), theMap);
  }

  @VisibleForTesting
  static InputRow parse(
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
    }
    catch (Exception e) {
      throw new ParseException(
          e,
          "Timestamp[%s] is unparseable! Event: %s",
          timestampSpec.getRawTimestamp(theMap),
          rawMapToPrint(theMap)
      );
    }
    if (timestamp == null) {
      throw new ParseException(
          "Timestamp[%s] is unparseable! Event: %s",
          timestampSpec.getRawTimestamp(theMap),
          rawMapToPrint(theMap)
      );
    }
    if (!Intervals.ETERNITY.contains(timestamp)) {
      throw new ParseException(
          "Encountered row with timestamp[%s] that cannot be represented as a long: [%s]",
          timestamp,
          rawMapToPrint(theMap)
      );
    }
    return new MapBasedInputRow(timestamp, dimensionsToUse, theMap);
  }

  @Nullable
  private static String rawMapToPrint(@Nullable Map<String, Object> rawMap)
  {
    if (rawMap == null) {
      return null;
    }
    final String input = rawMap.toString();
    return input.length() < 100 ? input : input.substring(0, 100) + "...";
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
