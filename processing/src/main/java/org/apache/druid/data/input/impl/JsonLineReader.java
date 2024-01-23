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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link JsonLineReader} reads input text line by line and tries to convert each text line to an JSON object.
 *
 * Since each text line is processed indepdently, if any exception is thrown when parsing one text line,
 * exception can be caught by callers to skip current line and continue to process next text line.
 *
 * This also means that each text line should be a well-formed JSON text, pretty-printed format is not allowed
 *
 */
public class JsonLineReader extends TextReader.Bytes
{
  private final ObjectFlattener<JsonNode> flattener;
  private final ObjectMapper mapper;

  JsonLineReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      JSONPathSpec flattenSpec,
      ObjectMapper mapper,
      boolean keepNullColumns
  )
  {
    super(inputRowSchema, source);
    this.flattener = ObjectFlatteners.create(
        flattenSpec,
        new JSONFlattenerMaker(
            keepNullColumns,
            inputRowSchema.getDimensionsSpec().useSchemaDiscovery()
        )
    );
    this.mapper = mapper;
  }

  @Override
  public List<InputRow> parseInputRows(byte[] line) throws IOException, ParseException
  {
    final JsonNode document = mapper.readValue(line, JsonNode.class);
    final Map<String, Object> flattened = flattener.flatten(document);
    return Collections.singletonList(MapInputRowParser.parse(getInputRowSchema(), flattened));
  }

  @Override
  public List<Map<String, Object>> toMap(byte[] intermediateRow) throws IOException
  {
    //noinspection unchecked
    return Collections.singletonList(mapper.readValue(intermediateRow, Map.class));
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
  public void processHeaderLine(byte[] line)
  {
    // do nothing
  }
}
