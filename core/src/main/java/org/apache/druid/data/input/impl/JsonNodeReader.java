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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * In contrast to {@link JsonLineReader} which processes input text line by line independently,
 * this class tries to split the input into a list of JsonNode objects, and then parses each JsonNode independently
 * into an InputRow.
 *
 * <p>
 * The input text can be:
 * 1. a JSON string of an object in a line or multiple lines(such as pretty-printed JSON text)
 * 2. multiple JSON object strings concated by white space character(s)
 * <p>
 * If an input string contains invalid JSON syntax, any valid JSON objects found prior to encountering the invalid
 * syntax will be successfully parsed, but parsing will not continue after the invalid syntax.
 * <p>
 */
public class JsonNodeReader extends IntermediateRowParsingReader<JsonNode>
{
  private final ObjectFlattener<JsonNode> flattener;
  private final ObjectMapper mapper;
  private final InputEntity source;
  private final InputRowSchema inputRowSchema;
  private final JsonFactory jsonFactory;

  JsonNodeReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      JSONPathSpec flattenSpec,
      ObjectMapper mapper,
      boolean keepNullColumns
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(keepNullColumns));
    this.mapper = mapper;
    this.jsonFactory = new JsonFactory();
  }

  @Override
  protected CloseableIterator<JsonNode> intermediateRowIterator() throws IOException
  {
    final String sourceString = IOUtils.toString(source.open(), StringUtils.UTF8_STRING);
    final List<JsonNode> jsonNodes = new ArrayList<>();
    try {
      JsonParser parser = jsonFactory.createParser(sourceString);
      final MappingIterator<JsonNode> delegate = mapper.readValues(parser, JsonNode.class);
      while (delegate.hasNext()) {
        jsonNodes.add(delegate.next());
      }
    }
    catch (Exception e) {
      //convert Jackson's JsonParseException into druid's exception for further processing
      //JsonParseException will be thrown from MappingIterator#hasNext or MappingIterator#next when input json text is ill-formed
      if (e.getCause() instanceof JsonParseException) {
        jsonNodes.add(
            new ParseExceptionMarkerJsonNode(
                new ParseException(sourceString, e, "Unable to parse row [%s]", sourceString)
            )
        );
      } else {
        throw e;
      }
    }

    if (CollectionUtils.isNullOrEmpty(jsonNodes)) {
      jsonNodes.add(
          new ParseExceptionMarkerJsonNode(
              new ParseException(
                  sourceString,
                  "Unable to parse [%s] as the intermediateRow resulted in empty input row",
                  sourceString
              )
          )
      );
    }
    return CloseableIterators.withEmptyBaggage(jsonNodes.iterator());
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(JsonNode intermediateRow) throws ParseException
  {
    if (intermediateRow instanceof ParseExceptionMarkerJsonNode) {
      throw ((ParseExceptionMarkerJsonNode) intermediateRow).getParseException();
    }
    final List<InputRow> inputRows = Collections.singletonList(
        MapInputRowParser.parse(inputRowSchema, flattener.flatten(intermediateRow))
    );

    if (CollectionUtils.isNullOrEmpty(inputRows)) {
      throw new ParseException(
          intermediateRow.toString(),
          "Unable to parse [%s] as the intermediateRow resulted in empty input row",
          intermediateRow.toString()
      );
    }
    return inputRows;
  }

  @Override
  protected List<Map<String, Object>> toMap(JsonNode intermediateRow) throws IOException
  {
    if (intermediateRow instanceof ParseExceptionMarkerJsonNode) {
      throw ((ParseExceptionMarkerJsonNode) intermediateRow).getParseException();
    }
    return Collections.singletonList(
        mapper.readValue(intermediateRow.toString(), new TypeReference<Map<String, Object>>()
        {
        })
    );
  }

  private static class ParseExceptionMarkerJsonNode extends ObjectNode
  {
    final ParseException parseException;

    public ParseExceptionMarkerJsonNode(ParseException pe)
    {
      super(null);
      this.parseException = pe;
    }

    public ParseException getParseException()
    {
      return parseException;
    }
  }
}
