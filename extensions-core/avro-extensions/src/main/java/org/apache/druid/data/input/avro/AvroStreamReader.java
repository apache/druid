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

package org.apache.druid.data.input.avro;

import com.google.common.collect.Iterators;
import org.apache.avro.generic.GenericRecord;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AvroStreamReader extends IntermediateRowParsingReader<GenericRecord>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final AvroBytesDecoder avroBytesDecoder;
  private final ObjectFlattener<GenericRecord> recordFlattener;

  AvroStreamReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      AvroBytesDecoder avroBytesDecoder,
      @Nullable JSONPathSpec flattenSpec,
      boolean binaryAsString,
      boolean extractUnionsByType
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.avroBytesDecoder = avroBytesDecoder;
    this.recordFlattener = ObjectFlatteners.create(flattenSpec, new AvroFlattenerMaker(false, binaryAsString, extractUnionsByType));
  }

  @Override
  protected CloseableIterator<GenericRecord> intermediateRowIterator() throws IOException
  {
    return CloseableIterators.withEmptyBaggage(
        Iterators.singletonIterator(avroBytesDecoder.parse(ByteBuffer.wrap(IOUtils.toByteArray(source.open()))))
    );
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }


  @Override
  protected List<InputRow> parseInputRows(GenericRecord intermediateRow) throws ParseException
  {
    return Collections.singletonList(
        MapInputRowParser.parse(
            inputRowSchema,
            recordFlattener.flatten(intermediateRow)
        )
    );
  }

  @Override
  protected List<Map<String, Object>> toMap(GenericRecord intermediateRow)
  {
    return Collections.singletonList(recordFlattener.toMap(intermediateRow));
  }
}
