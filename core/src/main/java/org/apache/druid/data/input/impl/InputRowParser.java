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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.collect.Utils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;

@Deprecated
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
   * Parse an input into list of {@link InputRow}. List can contains null for rows that should be thrown away,
   * or throws {@code ParseException} if the input is unparseable. This method should never return null otherwise
   * lots of things will break.
   */
  @NotNull
  default List<InputRow> parseBatch(T input)
  {
    return Utils.nullableListOf(parse(input));
  }

  /**
   * Parse an input into an {@link InputRow}. Return null if this input should be thrown away, or throws
   * {@code ParseException} if the input is unparseable.
   */
  @Deprecated
  @Nullable
  default InputRow parse(T input)
  {
    return null;
  }

  ParseSpec getParseSpec();

  InputRowParser withParseSpec(ParseSpec parseSpec);
}
