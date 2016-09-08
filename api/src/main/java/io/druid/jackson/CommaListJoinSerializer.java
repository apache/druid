/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.List;

/**
 */
public class CommaListJoinSerializer extends StdScalarSerializer<List<String>>
{
  private static final Joiner joiner = Joiner.on(",");

  protected CommaListJoinSerializer()
  {
    super(List.class, true);
  }

  @Override
  public void serialize(List<String> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonGenerationException
  {
    jgen.writeString(joiner.join(value));
  }
}
