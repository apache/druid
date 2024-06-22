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

import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowAdapters;

import java.io.IOException;

/**
 * InputSourceReader reads data from {@link InputSource} and returns a {@link CloseableIterator} of
 * {@link InputRow}s. See {@link InputSource} for an example usage.
 *
 * Implementations of this class can use {@link InputEntity} and {@link InputEntityReader}. {@link InputFormat}
 * can be useful to understand how to create an InputEntityReader.
 *
 * See {@link InputEntityIteratingReader} as an example.
 */
@UnstableApi
public interface InputSourceReader
{
  default CloseableIterator<InputRow> read() throws IOException
  {
    return read(null);
  }

  CloseableIterator<InputRow> read(InputStats inputStats) throws IOException;

  CloseableIterator<InputRowListPlusRawValues> sample() throws IOException;

  /**
   * Returns an adapter that can be used to read the rows from {@link #read()}.
   */
  default RowAdapter<InputRow> rowAdapter()
  {
    return RowAdapters.standardRow();
  }
}
