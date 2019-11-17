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

package org.apache.druid.data.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

/**
 * InputEntityReader knows how to parse data into {@link InputRow}.
 * This class is <i>stateful</i> and a new InputEntityReader should be created per {@link InputEntity}.
 *
 * @see IntermediateRowParsingReader
 * @see TextReader
 */
@UnstableApi
public interface InputEntityReader
{
  /**
   * Default JSON writer for sampler. This writer can be used to create an {@link InputRowListPlusJson}.
   * Note that this writer uses the default serializer of Jackson. You may want to create a custom writer
   * to serialize your custom types.
   */
  ObjectWriter DEFAULT_JSON_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

  CloseableIterator<InputRow> read() throws IOException;

  CloseableIterator<InputRowListPlusJson> sample() throws IOException;
}
