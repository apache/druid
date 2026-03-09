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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ThriftReader extends IntermediateRowParsingReader<byte[]>
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final String jarPath;
  private final String thriftClassName;
  private final ObjectFlattener<JsonNode> recordFlattener;

  private volatile Class<TBase> thriftClass = null;

  ThriftReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable String jarPath,
      String thriftClassName,
      @Nullable JSONPathSpec flattenSpec
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.jarPath = jarPath;
    this.thriftClassName = thriftClassName;
    this.recordFlattener = ObjectFlatteners.create(
        flattenSpec,
        new JSONFlattenerMaker(false, inputRowSchema.getDimensionsSpec().useSchemaDiscovery())
    );
  }

  @Override
  protected CloseableIterator<byte[]> intermediateRowIterator() throws IOException
  {
    return CloseableIterators.withEmptyBaggage(
        Iterators.singletonIterator(IOUtils.toByteArray(source.open()))
    );
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(byte[] intermediateRow) throws IOException, ParseException
  {
    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, toFlattenedMap(intermediateRow)));
  }

  @Override
  protected List<Map<String, Object>> toMap(byte[] intermediateRow) throws IOException
  {
    return Collections.singletonList(toFlattenedMap(intermediateRow));
  }

  private Map<String, Object> toFlattenedMap(byte[] bytes) throws ParseException
  {
    try {
      final Class<TBase> clazz = getThriftClass();
      final TBase tbase = clazz.newInstance();
      ThriftDeserialization.detectAndDeserialize(bytes, tbase);
      final String json = ThriftDeserialization.SERIALIZER_SIMPLE_JSON.get().toString(tbase);
      final JsonNode node = OBJECT_MAPPER.readTree(json);
      return recordFlattener.flatten(node);
    }
    catch (TException | IOException e) {
      throw new ParseException(null, e, "Failed to deserialize Thrift data");
    }
    catch (InstantiationException | IllegalAccessException e) {
      throw new ParseException(null, e, "Failed to instantiate Thrift class [%s]", thriftClassName);
    }
    catch (ClassNotFoundException e) {
      throw new ParseException(null, e, "Thrift class [%s] not found", thriftClassName);
    }
  }

  @SuppressWarnings("unchecked")
  private Class<TBase> getThriftClass() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    if (thriftClass == null) {
      final Class<TBase> clazz;
      if (jarPath != null) {
        File jar = new File(jarPath);
        URLClassLoader child = new URLClassLoader(
            new URL[]{jar.toURI().toURL()},
            this.getClass().getClassLoader()
        );
        clazz = (Class<TBase>) Class.forName(thriftClassName, true, child);
      } else {
        clazz = (Class<TBase>) Class.forName(thriftClassName);
      }
      // Verify the class can be instantiated
      clazz.newInstance();
      thriftClass = clazz;
    }
    return thriftClass;
  }
}
