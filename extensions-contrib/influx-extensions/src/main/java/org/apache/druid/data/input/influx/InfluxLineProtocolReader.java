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

package org.apache.druid.data.input.influx;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class InfluxLineProtocolReader extends TextReader.Strings
{
  static final String TIMESTAMP_KEY = "__ts";
  static final String MEASUREMENT_KEY = "measurement";

  private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\\\\"");
  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\\\([,= ])");

  @Nullable
  private final Set<String> measurementWhitelist;

  InfluxLineProtocolReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable Set<String> measurementWhitelist
  )
  {
    super(inputRowSchema, source);
    this.measurementWhitelist = measurementWhitelist;
  }

  @Override
  public List<InputRow> parseInputRows(String intermediateRow) throws ParseException
  {
    final Map<String, Object> parsed = parseLineToMap(intermediateRow);
    return Collections.singletonList(MapInputRowParser.parse(getInputRowSchema(), parsed));
  }

  @Override
  protected List<Map<String, Object>> toMap(String intermediateRow)
  {
    return Collections.singletonList(parseLineToMap(intermediateRow));
  }

  @Override
  public int getNumHeaderLinesToSkip()
  {
    return 0;
  }

  @Override
  public boolean needsToProcessHeaderLine()
  {
    return false;
  }

  @Override
  public void processHeaderLine(String line)
  {
    // no header lines in influx line protocol
  }

  private Map<String, Object> parseLineToMap(String input)
  {
    CharStream charStream = new ANTLRInputStream(input);
    InfluxLineProtocolLexer lexer = new InfluxLineProtocolLexer(charStream);
    TokenStream tokenStream = new CommonTokenStream(lexer);
    InfluxLineProtocolParser parser = new InfluxLineProtocolParser(tokenStream);

    List<InfluxLineProtocolParser.LineContext> lines = parser.lines().line();
    if (parser.getNumberOfSyntaxErrors() != 0) {
      throw new ParseException(input, "Unable to parse line.");
    }
    if (lines.size() != 1) {
      throw new ParseException(input, "Multiple lines present; unable to parse more than one per record.");
    }

    Map<String, Object> out = new LinkedHashMap<>();

    InfluxLineProtocolParser.LineContext line = lines.get(0);
    String measurement = parseIdentifier(line.identifier());

    if (!checkWhitelist(measurement)) {
      throw new ParseException(input, "Metric [%s] not whitelisted.", measurement);
    }

    out.put(MEASUREMENT_KEY, measurement);
    if (line.tag_set() != null) {
      line.tag_set().tag_pair().forEach(t -> parseTag(t, out));
    }

    line.field_set().field_pair().forEach(t -> parseField(t, out));

    if (line.timestamp() != null) {
      String timestamp = line.timestamp().getText();
      parseTimestamp(timestamp, out);
    }
    return out;
  }

  private static void parseTag(InfluxLineProtocolParser.Tag_pairContext tag, Map<String, Object> out)
  {
    String key = parseIdentifier(tag.identifier(0));
    String value = parseIdentifier(tag.identifier(1));
    out.put(key, value);
  }

  private static void parseField(InfluxLineProtocolParser.Field_pairContext field, Map<String, Object> out)
  {
    String key = parseIdentifier(field.identifier());
    InfluxLineProtocolParser.Field_valueContext valueContext = field.field_value();
    Object value;
    if (valueContext.NUMBER() != null) {
      value = parseNumber(valueContext.NUMBER().getText());
    } else if (valueContext.BOOLEAN() != null) {
      value = parseBool(valueContext.BOOLEAN().getText());
    } else {
      value = parseQuotedString(valueContext.QUOTED_STRING().getText());
    }
    out.put(key, value);
  }

  private static Object parseQuotedString(String text)
  {
    return BACKSLASH_PATTERN.matcher(text.substring(1, text.length() - 1)).replaceAll("\"");
  }

  private static Object parseNumber(String raw)
  {
    if (raw.endsWith("i")) {
      return Long.valueOf(raw.substring(0, raw.length() - 1));
    }
    return Double.valueOf(raw);
  }

  private static Object parseBool(String raw)
  {
    char first = raw.charAt(0);
    if (first == 't' || first == 'T') {
      return "true";
    } else {
      return "false";
    }
  }

  private static String parseIdentifier(InfluxLineProtocolParser.IdentifierContext ctx)
  {
    if (ctx.BOOLEAN() != null || ctx.NUMBER() != null) {
      return ctx.getText();
    }
    return IDENTIFIER_PATTERN.matcher(ctx.IDENTIFIER_STRING().getText()).replaceAll("$1");
  }

  private boolean checkWhitelist(String m)
  {
    return (measurementWhitelist == null) || measurementWhitelist.contains(m);
  }

  private static void parseTimestamp(String timestamp, Map<String, Object> dest)
  {
    // Influx timestamps come in nanoseconds; treat anything less than 1 ms as 0
    if (timestamp.length() < 7) {
      dest.put(TIMESTAMP_KEY, 0L);
    } else {
      timestamp = timestamp.substring(0, timestamp.length() - 6);
      final long timestampMillis = Long.valueOf(timestamp);
      dest.put(TIMESTAMP_KEY, timestampMillis);
    }
  }
}