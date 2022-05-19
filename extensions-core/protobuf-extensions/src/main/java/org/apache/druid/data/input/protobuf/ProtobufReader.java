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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
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
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProtobufReader extends IntermediateRowParsingReader<DynamicMessage>
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final JSONPathSpec flattenSpec;
  private final ObjectFlattener<JsonNode> recordFlattener;
  private final ProtobufBytesDecoder protobufBytesDecoder;

  ProtobufReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      ProtobufBytesDecoder protobufBytesDecoder,
      JSONPathSpec flattenSpec
  )
  {
    if (flattenSpec == null) {
      this.inputRowSchema = new ProtobufInputRowSchema(inputRowSchema);
      this.recordFlattener = null;
    } else {
      this.inputRowSchema = inputRowSchema;
      this.recordFlattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker(true));
    }

    this.source = source;
    this.protobufBytesDecoder = protobufBytesDecoder;
    this.flattenSpec = flattenSpec;
  }

  @Override
  protected CloseableIterator<DynamicMessage> intermediateRowIterator() throws IOException
  {
    return CloseableIterators.withEmptyBaggage(
        Iterators.singletonIterator(protobufBytesDecoder.parse(ByteBuffer.wrap(IOUtils.toByteArray(source.open()))))
    );
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(DynamicMessage intermediateRow) throws ParseException, JsonProcessingException
  {
    Map<String, Object> record;

    if (flattenSpec == null || JSONPathSpec.DEFAULT.equals(flattenSpec)) {
      try {
        record = CollectionUtils.mapKeys(intermediateRow.getAllFields(), k -> k.getJsonName());
      }
      catch (Exception ex) {
        throw new ParseException(null, ex, "Protobuf message could not be parsed");
      }
    } else {
      try {
        String json = JsonFormat.printer().print(intermediateRow);
        record = recordFlattener.flatten(OBJECT_MAPPER.readValue(json, JsonNode.class));
      }
      catch (InvalidProtocolBufferException e) {
        throw new ParseException(null, e, "Protobuf message could not be parsed");
      }
    }

    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, record));
  }

  @Override
  protected List<Map<String, Object>> toMap(DynamicMessage intermediateRow) throws JsonProcessingException, InvalidProtocolBufferException
  {
    return Collections.singletonList(new ObjectMapper().readValue(JsonFormat.printer().print(intermediateRow), Map.class));
  }
}
