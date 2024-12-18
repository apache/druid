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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.data.VByte;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ColumnSerializerUtils
{
  public static final String STRING_DICTIONARY_FILE_NAME = "__stringDictionary";
  public static final String LONG_DICTIONARY_FILE_NAME = "__longDictionary";
  public static final String DOUBLE_DICTIONARY_FILE_NAME = "__doubleDictionary";
  public static final String ARRAY_DICTIONARY_FILE_NAME = "__arrayDictionary";
  public static final String ARRAY_ELEMENT_DICTIONARY_FILE_NAME = "__arrayElementDictionary";
  public static final String ENCODED_VALUE_COLUMN_FILE_NAME = "__encodedColumn";
  public static final String LONG_VALUE_COLUMN_FILE_NAME = "__longColumn";
  public static final String DOUBLE_VALUE_COLUMN_FILE_NAME = "__doubleColumn";
  public static final String BITMAP_INDEX_FILE_NAME = "__valueIndexes";
  public static final String ARRAY_ELEMENT_BITMAP_INDEX_FILE_NAME = "__arrayElementIndexes";
  public static final String NULL_BITMAP_FILE_NAME = "__nullIndex";

  public static final ObjectMapper SMILE_MAPPER;

  static {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper mapper = new DefaultObjectMapper(smileFactory, null);
    mapper.getFactory().setCodec(mapper);
    mapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    SMILE_MAPPER = mapper;
  }

  public static void writeInternal(FileSmoosher smoosher, Serializer serializer, String columnName, String fileName)
      throws IOException
  {
    smoosher.serializeAs(getInternalFileName(columnName, fileName), serializer);
  }

  public static String getInternalFileName(String fileNameBase, String field)
  {
    return fileNameBase + "." + field;
  }

  /**
   * Convert a String to a ByteBuffer with a variable size length prepended to it.
   * @param stringVal the value to store in the ByteBuffer
   * @return ByteBuffer with the string converted to utf8 bytes and stored with a variable size length int prepended
   */
  public static ByteBuffer stringToUtf8InVSizeByteBuffer(String stringVal)
  {
    final byte[] bytes = StringUtils.toUtf8(stringVal);
    final int length = VByte.computeIntSize(bytes.length);
    final ByteBuffer buffer = ByteBuffer.allocate(length + bytes.length).order(ByteOrder.nativeOrder());
    VByte.writeInt(buffer, bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  /**
   * Writes a {@link Serializer} to a 'smoosh file' which contains the contents of this single serializer, with the
   * serializer writing to an internal file specified by the name argument, returning a {@link SmooshedFileMapper}
   */
  public static SmooshedFileMapper mapSerializer(File smooshFile, Serializer writer, String name)
  {
    try (
        final FileSmoosher smoosher = new FileSmoosher(smooshFile);
        final SmooshedWriter smooshedWriter = smoosher.addWithSmooshedWriter(
            name,
            writer.getSerializedSize()
        )
    ) {
      writer.writeTo(smooshedWriter, smoosher);
      smooshedWriter.close();
      smoosher.close();
      return SmooshedFileMapper.load(smooshFile);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
