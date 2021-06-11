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

package org.apache.druid.data.input.thrift;

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
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;

public class ThriftReader extends IntermediateRowParsingReader<String>
{
  private final InputEntity source;
  private final String jarPath;
  private final String thriftClassName;
  private volatile Class<TBase> thriftClass = null;
  private final ObjectMapper objectMapper;
  private final ObjectFlattener<JsonNode> flattener;
  private final InputRowSchema inputRowSchema;


  ThriftReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      String jarPath,
      String thriftClassName,
      JSONPathSpec flattenSpec
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(true));
    this.source = source;
    this.jarPath = jarPath;
    this.thriftClassName = thriftClassName;
    this.objectMapper = new ObjectMapper();
    if (thriftClass == null) {
      try {
        thriftClass = getThriftClass();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      catch (InstantiationException e) {
        e.printStackTrace();
      }
    }
  }

  public Class<TBase> getThriftClass()
      throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
  {
    final Class<TBase> thrift;
    if (jarPath != null) {
      File jar = new File(jarPath);
      URLClassLoader child = new URLClassLoader(
          new URL[] {jar.toURI().toURL()},
          this.getClass().getClassLoader()
      );
      thrift = (Class<TBase>) Class.forName(thriftClassName, true, child);
    } else {
      thrift = (Class<TBase>) Class.forName(thriftClassName);
    }
    thrift.newInstance();
    return thrift;
  }

  @Override
  protected CloseableIterator<String> intermediateRowIterator() throws IOException
  {
    final String json;
    try {
      final byte[] bytes = IOUtils.toByteArray(source.open());
      TBase o = thriftClass.newInstance();
      ThriftDeserialization.detectAndDeserialize(bytes, o);
      json = ThriftDeserialization.SERIALIZER_SIMPLE_JSON.get().toString(o);
    }
    catch (IllegalAccessException | InstantiationException | TException e) {
      throw new IAE("some thing wrong with your thrift?");
    }

    return CloseableIterators.withEmptyBaggage(
        Iterators.singletonIterator(json)
    );
  }

  @Override
  protected List<InputRow> parseInputRows(String intermediateRow) throws IOException, ParseException
  {
    final List<InputRow> inputRows;
    try (JsonParser parser = new JsonFactory().createParser(intermediateRow)) {
      final MappingIterator<JsonNode> delegate = this.objectMapper.readValues(parser, JsonNode.class);
      inputRows = FluentIterable.from(() -> delegate)
          .transform(jsonNode -> MapInputRowParser.parse(inputRowSchema, flattener.flatten(jsonNode)))
          .toList();
    }
    catch (RuntimeException e) {
      if (e.getCause() instanceof JsonParseException) {
        throw new ParseException(e, "Unable to parse row [%s]", intermediateRow);
      }
      throw e;
    }
    if (CollectionUtils.isNullOrEmpty(inputRows)) {
      throw new ParseException("Unable to parse [%s] as the intermediateRow resulted in empty input row", intermediateRow);
    }
    return inputRows;
  }

  @Override
  protected List<Map<String, Object>> toMap(String intermediateRow) throws IOException
  {
    try (JsonParser parser = new JsonFactory().createParser(intermediateRow)) {
      final MappingIterator<Map> delegate = objectMapper.readValues(parser, Map.class);
      return FluentIterable.from(() -> delegate)
          .transform(map -> (Map<String, Object>) map)
          .toList();
    }
    catch (RuntimeException e) {
      if (e.getCause() instanceof JsonParseException) {
        throw new ParseException(e, "Unable to parse row [%s]", intermediateRow);
      }
      throw e;
    }
  }
}
