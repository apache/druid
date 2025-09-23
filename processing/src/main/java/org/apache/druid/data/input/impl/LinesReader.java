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

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.util.List;
import java.util.Map;

/**
 * Reader for {@link LinesInputFormat}.
 */
public class LinesReader extends TextReader.Strings
{
  private static final String LINE_COLUMN = "line";

  LinesReader(
      InputRowSchema inputRowSchema,
      InputEntity source
  )
  {
    super(inputRowSchema, source);
  }

  @Override
  public List<InputRow> parseInputRows(String intermediateRow) throws ParseException
  {
    return List.of(MapInputRowParser.parse(getInputRowSchema(), parseLine(intermediateRow)));
  }

  @Override
  protected List<Map<String, Object>> toMap(String intermediateRow)
  {
    return List.of(parseLine(intermediateRow));
  }

  private Map<String, Object> parseLine(String line)
  {
    return Map.of(LINE_COLUMN, line);
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
    // Nothing to do.
  }
}
