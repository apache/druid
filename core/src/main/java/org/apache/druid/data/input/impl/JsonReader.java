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
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * <pre>
 * In constract to {@link JsonLineReader} which processes input text line by line independently,
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
 * </pre>
 */
public class JsonReader implements InputEntityReader
{
  private final ObjectFlattener<JsonNode> flattener;
  private final ObjectMapper mapper;
  private final InputEntity source;
  private final InputRowSchema inputRowSchema;

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
    this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(keepNullColumns));
    this.mapper = mapper;
  }

  abstract class Iterator<JsonObjType, TargetObjType> implements CloseableIterator<TargetObjType>
  {
    private final JsonParser parser;
    private final MappingIterator<JsonObjType> delegate;

    Iterator(Class<JsonObjType> clazz) throws IOException
    {
      parser = new JsonFactory().createParser(source.open());
      delegate = mapper.readValues(
          parser,
          clazz
      );
    }

    @Override
    public void close() throws IOException
    {
      parser.close();
    }

    @Override
    public boolean hasNext()
    {
      try {
        return delegate.hasNext();
      }
      catch (RuntimeException e) {
        if (e.getCause() instanceof JsonParseException) {
          // skip the ill-formed input so that callers(such as index task) won't fail
          return false;
        }

        // rethrow the unknown exception
        throw e;
      }
    }

    @Override
    public TargetObjType next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try {
        return toObject(delegate.next());
      }
      catch (RuntimeException e) {
        if (e.getCause() instanceof JsonParseException) {
          //rethrow parse exception so that it can be processed by callers(such as ingest task) in a unified way
          throw new ParseException(e.getCause(), e.getMessage());
        }

        // rethrow the unknown exception
        throw e;
      }
    }

    protected abstract TargetObjType toObject(JsonObjType node);
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return new Iterator<JsonNode, InputRow>(JsonNode.class)
    {
      @Override
      protected InputRow toObject(JsonNode node)
      {
        return toInputRow(node);
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return new Iterator<Map, InputRowListPlusRawValues>(Map.class)
    {
      @Override
      protected InputRowListPlusRawValues toObject(Map rawColumns)
      {
        try {
          return InputRowListPlusRawValues.of(
              Collections.singletonList(toInputRow(rawColumns)),
              rawColumns
          );
        }
        catch (ParseException e) {
          return InputRowListPlusRawValues.of(rawColumns, e);
        }
      }
    };
  }

  private InputRow toInputRow(Map map)
  {
    JsonNode node = mapper.valueToTree(map);
    return toInputRow(node);
  }

  private InputRow toInputRow(JsonNode node)
  {
    return MapInputRowParser.parse(inputRowSchema, flattener.flatten(node));
  }
}
