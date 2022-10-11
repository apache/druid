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
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.Objects;

public class InlineDescriptorProtobufBytesDecoder extends DescriptorBasedProtobufBytesDecoder
{
  private final String descriptorString;

  @JsonCreator
  public InlineDescriptorProtobufBytesDecoder(
      @JsonProperty("descriptorString") String descriptorString,
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    super(protoMessageType);
    Preconditions.checkNotNull(descriptorString);
    this.descriptorString = descriptorString;
    initDescriptor();
  }

  @JsonProperty
  public String getDescriptorString()
  {
    return descriptorString;
  }

  @Override
  protected DynamicSchema generateDynamicSchema()
  {
    try {
      byte[] decodedDesc = StringUtils.decodeBase64String(descriptorString);
      return DynamicSchema.parseFrom(decodedDesc);
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Descriptor string does not have valid Base64 encoding.");
    }
    catch (Descriptors.DescriptorValidationException e) {
      throw new ParseException(null, e, "Invalid descriptor string: " + descriptorString);
    }
    catch (IOException e) {
      throw new ParseException(null, e, "Cannot read descriptor string: " + descriptorString);
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
    InlineDescriptorProtobufBytesDecoder that = (InlineDescriptorProtobufBytesDecoder) o;
    return Objects.equals(getDescriptorString(), that.getDescriptorString());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), getDescriptorString());
  }
}
