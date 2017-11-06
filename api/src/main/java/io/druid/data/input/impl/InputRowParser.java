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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.InputRow;
import io.druid.guice.annotations.ExtensionPoint;

import javax.annotation.Nullable;

@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class),
    @JsonSubTypes.Type(name = "map", value = MapInputRowParser.class),
    @JsonSubTypes.Type(name = "noop", value = NoopInputRowParser.class)
})
public interface InputRowParser<T>
{
  /**
   * Parse an input into an {@link InputRow}. Return null if this input should be thrown away, or throws
   * {@code ParseException} if the input is unparseable.
   */
  @Nullable
  InputRow parse(T input);

  ParseSpec getParseSpec();

  InputRowParser withParseSpec(ParseSpec parseSpec);
}
