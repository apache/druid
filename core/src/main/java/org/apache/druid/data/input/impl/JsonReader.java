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


  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final MappingIterator<JsonNode> delegate = mapper.readValues(
        new JsonFactory().createParser(this.source.open()),
        JsonNode.class
    );

    return new CloseableIterator<InputRow>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        return toInputRow(delegate.next());
      }

      @Override
      public void close()
      {
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final MappingIterator<Map> delegate = mapper.readValues(
        new JsonFactory().createParser(this.source.open()),
        Map.class
    );

    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final Map<String, Object> rawColumns;
        try {
          rawColumns = delegate.next();
        }
        catch (Exception e) {
          return InputRowListPlusRawValues.of(null, new ParseException(e, "Unable to parse row into JSON"));
        }
        try {
          return InputRowListPlusRawValues.of(Collections.singletonList(toInputRow(rawColumns)), rawColumns);
        }
        catch (ParseException e) {
          return InputRowListPlusRawValues.of(rawColumns, e);
        }
      }

      @Override
      public void close()
      {
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
