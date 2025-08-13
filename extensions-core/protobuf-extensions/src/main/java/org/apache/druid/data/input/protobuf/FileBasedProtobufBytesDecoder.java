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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class FileBasedProtobufBytesDecoder extends DescriptorBasedProtobufBytesDecoder
{
  private final String descriptorFilePath;

  @JsonCreator
  public FileBasedProtobufBytesDecoder(
      @JsonProperty("descriptor") String descriptorFilePath,
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    super(protoMessageType);

    Preconditions.checkNotNull(descriptorFilePath);
    this.descriptorFilePath = descriptorFilePath;

    initDescriptor();
  }

  @JsonProperty("descriptor")
  public String getDescriptorFilePath()
  {
    return descriptorFilePath;
  }

  @Override
  protected DescriptorProtos.FileDescriptorSet generateFileDescriptorSet()
  {
    try (InputStream fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFilePath)) {
      if (fin == null) {
        throw new ParseException(descriptorFilePath, "Descriptor not found in class path: %s", descriptorFilePath);
      }

      final var descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
      if (descriptorSet.getFileCount() == 0) {
        throw new ParseException(null, "No file descriptors found in the descriptor set.");
      }

      return descriptorSet;
    }
    catch (IOException e) {
      throw new ParseException(descriptorFilePath, e, "Failed to initialize descriptor");
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    FileBasedProtobufBytesDecoder that = (FileBasedProtobufBytesDecoder) o;
    return Objects.equals(descriptorFilePath, that.descriptorFilePath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), descriptorFilePath);
  }
}
