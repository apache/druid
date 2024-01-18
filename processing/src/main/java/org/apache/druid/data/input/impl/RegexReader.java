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

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.java.util.common.parsers.Parsers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexReader extends TextReader.Strings
{
  private final String pattern;
  private final Pattern compiledPattern;
  private final Function<String, Object> multiValueFunction;

  private List<String> columns;

  RegexReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      String pattern,
      Pattern compiledPattern,
      @Nullable String listDelimiter,
      @Nullable List<String> columns
  )
  {
    super(inputRowSchema, source);
    this.pattern = pattern;
    this.compiledPattern = compiledPattern;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));
    this.columns = columns;
  }

  @Override
  public List<InputRow> parseInputRows(String intermediateRow) throws ParseException
  {
    return Collections.singletonList(MapInputRowParser.parse(getInputRowSchema(), parseLine(intermediateRow)));
  }

  @Override
  protected List<Map<String, Object>> toMap(String intermediateRow)
  {
    return Collections.singletonList(parseLine(intermediateRow));
  }

  private Map<String, Object> parseLine(String line)
  {
    try {
      final Matcher matcher = compiledPattern.matcher(line);

      if (!matcher.matches()) {
        throw new ParseException(line, "Incorrect Regex: %s . No match found.", pattern);
      }

      final List<String> values = new ArrayList<>();
      for (int i = 1; i <= matcher.groupCount(); i++) {
        values.add(matcher.group(i));
      }

      if (columns == null) {
        columns = ParserUtils.generateFieldNames(matcher.groupCount());
      }

      return Utils.zipMapPartial(columns, Iterables.transform(values, multiValueFunction));
    }
    catch (Exception e) {
      throw new ParseException(line, e, "Unable to parse row [%s]", line);
    }
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
    // do nothing
  }
}
