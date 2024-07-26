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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

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
  public List<InputRow> parseBatch(Map<String, Object> theMap)
  {
    return ImmutableList.of(
        parse(
            parseSpec.getTimestampSpec(),
            parseSpec.getDimensionsSpec(),
            theMap
        )
    );
  }

  public static InputRow parse(InputRowSchema inputRowSchema, Map<String, Object> theMap) throws ParseException
  {
    return parse(inputRowSchema.getTimestampSpec(), inputRowSchema.getDimensionsSpec(), theMap);
  }

  @VisibleForTesting
  static InputRow parse(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      Map<String, Object> theMap
  ) throws ParseException
  {
    final List<String> dimensionsToUse = findDimensions(
        timestampSpec,
        dimensionsSpec,
        theMap == null ? Collections.emptySet() : theMap.keySet()
    );

    return parse(timestampSpec, dimensionsToUse, theMap);
  }

  public static InputRow parse(
      TimestampSpec timestampSpec,
      List<String> dimensions,
      Map<String, Object> theMap
  ) throws ParseException
  {
    final DateTime timestamp = parseTimestamp(timestampSpec, theMap);
    return new MapBasedInputRow(timestamp, dimensions, theMap);
  }

  /**
   * Finds the final set of dimension names to use for {@link InputRow}.
   * There are 3 cases here.
   *
   * 1) If {@link DimensionsSpec#isIncludeAllDimensions()} is set, the returned list includes _both_
   * {@link DimensionsSpec#getDimensionNames()} and the dimensions in the given map ({@code rawInputRow#keySet()}).
   * 2) If isIncludeAllDimensions is not set and {@link DimensionsSpec#getDimensionNames()} is not empty,
   * the dimensions in dimensionsSpec is returned.
   * 3) If isIncludeAllDimensions is not set and {@link DimensionsSpec#getDimensionNames()} is empty,
   * the dimensions in the given map is returned.
   *
   * In any case, the returned list does not include any dimensions in {@link DimensionsSpec#getDimensionExclusions()}
   * or {@link TimestampSpec#getTimestampColumn()}.
   */
  public static List<String> findDimensions(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      Set<String> fields
  )
  {
    final String timestampColumn = timestampSpec.getTimestampColumn();
    final Set<String> exclusions = dimensionsSpec.getDimensionExclusions();
    if (dimensionsSpec.isIncludeAllDimensions() || dimensionsSpec.useSchemaDiscovery()) {
      LinkedHashSet<String> dimensions = new LinkedHashSet<>(dimensionsSpec.getDimensionNames());
      for (String field : fields) {
        if (timestampColumn.equals(field) || exclusions.contains(field)) {
          continue;
        }
        dimensions.add(field);
      }
      return new ArrayList<>(dimensions);
    } else {
      if (!dimensionsSpec.getDimensionNames().isEmpty()) {
        return dimensionsSpec.getDimensionNames();
      } else {
        List<String> dimensions = new ArrayList<>();
        for (String field : fields) {
          if (timestampColumn.equals(field) || exclusions.contains(field)) {
            continue;
          }
          dimensions.add(field);
        }
        return dimensions;
      }
    }
  }

  public static DateTime parseTimestamp(TimestampSpec timestampSpec, Map<String, Object> theMap)
  {
    return parseTimestampOrThrowParseException(
        timestampSpec.getRawTimestamp(theMap),
        timestampSpec,
        () -> theMap
    );
  }

  /**
   * Given a plain Java Object, extract a timestamp from it using the provided {@link TimestampSpec}, or throw
   * a {@link ParseException} if we can't.
   *
   * @param timeValue      object to interpret as timestmap
   * @param timestampSpec  timestamp spec
   * @param rawMapSupplier supplier of the original raw data that this object came from. Used for error messages.
   */
  public static DateTime parseTimestampOrThrowParseException(
      final Object timeValue,
      final TimestampSpec timestampSpec,
      final Supplier<Map<String, ?>> rawMapSupplier
  )
  {
    final DateTime timestamp;

    try {
      timestamp = timestampSpec.parseDateTime(timeValue);
    }
    catch (Exception e) {
      String rawMap = rawMapToPrint(rawMapSupplier.get());
      throw new ParseException(
          rawMap,
          e,
          "Timestamp[%s] is unparseable! Event: %s",
          timeValue,
          rawMap
      );
    }
    if (timestamp == null) {
      String rawMap = rawMapToPrint(rawMapSupplier.get());
      throw new ParseException(
          rawMap,
          "Timestamp[%s] is unparseable! Event: %s",
          timeValue,
          rawMap
      );
    }
    if (!Intervals.ETERNITY.contains(timestamp)) {
      String rawMap = rawMapToPrint(rawMapSupplier.get());
      throw new ParseException(
          rawMap,
          "Encountered row with timestamp[%s] that cannot be represented as a long: [%s]",
          timestamp,
          rawMap
      );
    }
    return timestamp;
  }

  @Nullable
  private static String rawMapToPrint(@Nullable Map<String, ?> rawMap)
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
