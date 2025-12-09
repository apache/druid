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
import java.net.MalformedURLException;
import java.net.URL;
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

    initializeDescriptor();
  }

  @JsonProperty("descriptor")
  public String getDescriptorFilePath()
  {
    return descriptorFilePath;
  }

  @Override
  protected DescriptorProtos.FileDescriptorSet loadFileDescriptorSet()
  {
    InputStream fin;
    DescriptorProtos.FileDescriptorSet descriptorSet;
    try {
      fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFilePath);
      if (fin == null) {
        URL url;
        try {
          url = new URL(descriptorFilePath);
        }
        catch (MalformedURLException e) {
          throw new ParseException(
              descriptorFilePath,
              e,
              "Descriptor not found in class path or malformed URL: [%s]", descriptorFilePath
              );
        }
        try (InputStream urlIn = url.openConnection().getInputStream()) {
          if (urlIn == null) {
            throw new ParseException(
                descriptorFilePath,
                "Descriptor not found at URL: [%s]", descriptorFilePath
            );
          }
          descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(urlIn);
        }
      } else {
        descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
      }

      if (descriptorSet.getFileCount() == 0) {
        throw new ParseException(null, "No file descriptors found in the descriptor set");
      }

      return descriptorSet;
    }
    catch (IOException e) {
      throw new ParseException(descriptorFilePath, e, "Failed to initialize descriptor at [%s]", descriptorFilePath);
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
