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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link Iterator} for {@link InputRow}s from a {@link Firehose}.
 */
public class InputRowIterator implements Iterator<InputRow>
{
  @FunctionalInterface
  public interface InputRowHandler
  {
    /**
     * @return True if inputRow was successfully handled and no further processing is needed
     */
    boolean handle(InputRow inputRow);
  }

  private final Firehose firehose;
  private final List<InputRowHandler> inputRowHandlers;

  /**
   * @param firehose         Source of {@link InputRow}s
   * @param inputRowHandlers Before yielding the next {@link InputRow}, each {@link InputRowHandler} is sequentially
   *                         applied to the {@link InputRow} until one of them returns true or all of the handlers are
   *                         applied.
   */
  public InputRowIterator(
      Firehose firehose,
      List<InputRowHandler> inputRowHandlers
  )
  {
    this.firehose = firehose;
    this.inputRowHandlers = inputRowHandlers;
  }

  @Override
  public boolean hasNext()
  {
    try {
      return firehose.hasMore();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @return Next {@link InputRow} or null if row was successfully handled by an {@link InputRowHandler}.
   */
  @Override
  @Nullable
  public InputRow next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    InputRow inputRow;
    try {
      inputRow = firehose.nextRow();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    for (InputRowHandler inputRowHandler : inputRowHandlers) {
      if (inputRowHandler.handle(inputRow)) {
        return null;
      }
    }

    return inputRow;
  }
}

