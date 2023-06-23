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

import com.google.common.collect.Iterators;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProtobufReader extends IntermediateRowParsingReader<DynamicMessage>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final ObjectFlattener<Map<String, Object>> recordFlattener;
  private final ProtobufBytesDecoder protobufBytesDecoder;

  ProtobufReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      ProtobufBytesDecoder protobufBytesDecoder,
      JSONPathSpec flattenSpec
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.recordFlattener = ObjectFlatteners.create(
        flattenSpec,
        new ProtobufFlattenerMaker(inputRowSchema.getDimensionsSpec().useSchemaDiscovery())
    );
    this.source = source;
    this.protobufBytesDecoder = protobufBytesDecoder;
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
  protected List<InputRow> parseInputRows(DynamicMessage intermediateRow) throws ParseException
  {
    final Map<String, Object> record;
    final Map<String, Object> plainJava = convertMessage(intermediateRow);
    record = recordFlattener.flatten(plainJava);
    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, record));
  }

  @Override
  protected List<Map<String, Object>> toMap(DynamicMessage intermediateRow)
  {
    return Collections.singletonList(convertMessage(intermediateRow));
  }

  private static Map<String, Object> convertMessage(Message msg)
  {
    try {
      return ProtobufConverter.convertMessage(msg);
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
  }
}
