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

package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.protobuf.util.JsonFormat;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProtobufReader extends IntermediateRowParsingReader<String>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final ObjectFlattener<JsonNode> recordFlattener;
  private final ProtobufBytesDecoder protobufBytesDecoder;

  ProtobufReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      ProtobufBytesDecoder protobufBytesDecoder,
      JSONPathSpec flattenSpec
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.protobufBytesDecoder = protobufBytesDecoder;
    this.recordFlattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(true));
  }

  @Override
  protected CloseableIterator<String> intermediateRowIterator() throws IOException
  {
    return CloseableIterators.withEmptyBaggage(
        Iterators.singletonIterator(JsonFormat.printer().print(protobufBytesDecoder.parse(ByteBuffer.wrap(IOUtils.toByteArray(source.open())))
        )));
  }

  @Override
  protected List<InputRow> parseInputRows(String intermediateRow) throws ParseException, JsonProcessingException
  {
    JsonNode document = new ObjectMapper().readValue(intermediateRow, JsonNode.class);
    final Map<String, Object> flattened = recordFlattener.flatten(document);
    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, flattened));
  }

  @Override
  protected List<Map<String, Object>> toMap(String intermediateRow) throws JsonProcessingException
  {
    return Collections.singletonList(new ObjectMapper().readValue(intermediateRow, Map.class));
  }
}
