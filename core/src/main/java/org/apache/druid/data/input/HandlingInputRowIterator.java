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

import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Decorated {@link CloseableIterator<InputRow>} that can process rows with {@link InputRowHandler}s.
 */
public class HandlingInputRowIterator implements CloseableIterator<InputRow>
{
  @FunctionalInterface
  public interface InputRowHandler
  {
    /**
     * @return True if inputRow was successfully handled and no further processing is needed
     */
    boolean handle(InputRow inputRow);
  }

  private final CloseableIterator<InputRow> delegate;
  private final List<InputRowHandler> inputRowHandlers;

  /**
   * @param inputRowIterator Source of {@link InputRow}s
   * @param inputRowHandlers Before yielding the next {@link InputRow}, each {@link InputRowHandler} is sequentially
   *                         applied to the {@link InputRow} until one of them returns true or all of the handlers are
   *                         applied.
   */
  public HandlingInputRowIterator(
      CloseableIterator<InputRow> inputRowIterator,
      List<InputRowHandler> inputRowHandlers
  )
  {
    this.delegate = inputRowIterator;
    this.inputRowHandlers = inputRowHandlers;
  }

  @Override
  public boolean hasNext()
  {
    return delegate.hasNext();
  }

  /**
   * @return Next {@link InputRow} or null if row was successfully handled by an {@link InputRowHandler}.
   */
  @Override
  @Nullable
  public InputRow next()
  {
    InputRow inputRow = delegate.next();

    // NOTE: This loop invokes a virtual call per input row, which may have significant overhead for large inputs
    // (e.g. InputSourceProcessor). If performance suffers, this implementation or the clients will need to change.
    for (InputRowHandler inputRowHandler : inputRowHandlers) {
      if (inputRowHandler.handle(inputRow)) {
        return null;
      }
    }

    return inputRow;
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}

