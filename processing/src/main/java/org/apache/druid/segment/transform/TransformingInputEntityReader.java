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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

public class TransformingInputEntityReader implements InputEntityReader
{
  private final InputEntityReader delegate;
  private final Transformer transformer;

  public TransformingInputEntityReader(InputEntityReader delegate, Transformer transformer)
  {
    this.delegate = delegate;
    this.transformer = transformer;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return delegate.read().map(transformer::transform);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return delegate.sample().map(transformer::transform);
  }
}
