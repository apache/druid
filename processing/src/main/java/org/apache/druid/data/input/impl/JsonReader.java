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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * In contrast to {@link JsonLineReader} which processes input text line by line independently,
 * this class tries to parse the input text as a whole to an array of objects.
 *
 * The input text can be:
 * 1. a JSON string of an object in a line or multiple lines(such as pretty-printed JSON text)
 * 2. multiple JSON object strings concated by white space character(s)
 *
 * For case 2, what should be noticed is that if an exception is thrown when parsing one JSON string,
 * the rest JSON text will all be ignored
 *
 * For more information, see: https://github.com/apache/druid/pull/10383
 */
public class JsonReader extends IntermediateRowParsingReader<InputEntity>
{
  private final ObjectFlattener<JsonNode> flattener;
  private final ObjectMapper mapper;
  private final InputEntity source;
  private final InputRowSchema inputRowSchema;
  private final JsonFactory jsonFactory;

  JsonReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      JSONPathSpec flattenSpec,
      ObjectMapper mapper,
      boolean keepNullColumns
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.flattener = ObjectFlatteners.create(
        flattenSpec,
        new JSONFlattenerMaker(
            keepNullColumns,
            inputRowSchema.getDimensionsSpec().useSchemaDiscovery()
        )
    );
    this.mapper = mapper;
    this.jsonFactory = new JsonFactory();
  }

  @Override
  protected CloseableIterator<InputEntity> intermediateRowIterator()
  {
    return CloseableIterators.withEmptyBaggage(Iterators.singletonIterator(source));
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected String intermediateRowAsString(@Nullable InputEntity entity)
  {
    if (entity == null) {
      return "null";
    } else {
      try {
        return IOUtils.toString(entity.open(), StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected List<InputRow> parseInputRows(InputEntity entity) throws IOException, ParseException
  {
    final List<InputRow> inputRows = new ArrayList<>();
    try (JsonParser parser = jsonFactory.createParser(entity.open())) {
      final MappingIterator<JsonNode> delegate = mapper.readValues(parser, JsonNode.class);
      while (delegate.hasNext()) {
        final JsonNode row = delegate.next();
        inputRows.add(MapInputRowParser.parse(inputRowSchema, flattener.flatten(row)));
      }
    }
    catch (RuntimeException e) {
      //convert Jackson's JsonParseException into druid's exception for further processing
      //JsonParseException will be thrown from MappingIterator#hasNext or MappingIterator#next when input json text is ill-formed
      if (e.getCause() instanceof JsonParseException) {
        final String rowAsString = IOUtils.toString(entity.open(), StandardCharsets.UTF_8);
        throw new ParseException(rowAsString, e, "Unable to parse row [%s]", rowAsString);
      }

      //throw unknown exception
      throw e;
    }

    if (inputRows.isEmpty()) {
      final String rowAsString = IOUtils.toString(entity.open(), StandardCharsets.UTF_8);
      throw new ParseException(
          rowAsString,
          "Unable to parse [%s] as the intermediateRow resulted in empty input row",
          rowAsString
      );
    }

    return inputRows;
  }

  @Override
  protected List<Map<String, Object>> toMap(InputEntity entity) throws IOException
  {
    try (JsonParser parser = jsonFactory.createParser(entity.open())) {
      final MappingIterator<Map> delegate = mapper.readValues(parser, Map.class);
      return FluentIterable.from(() -> delegate)
                           .transform(map -> (Map<String, Object>) map)
                           .toList();
    }
    catch (RuntimeException e) {
      //convert Jackson's JsonParseException into druid's exception for further processing
      //JsonParseException will be thrown from MappingIterator#hasNext or MappingIterator#next when input json text is ill-formed
      if (e.getCause() instanceof JsonParseException) {
        final String rowAsString = IOUtils.toString(entity.open(), StandardCharsets.UTF_8);
        throw new ParseException(rowAsString, e, "Unable to parse row [%s]", rowAsString);
      }

      //throw unknown exception
      throw e;
    }
  }
}
