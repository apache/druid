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

package org.apache.druid.msq.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;

/**
 * An {@link InputSource} that returns nothing (no rows).
 */
@JsonTypeName("nil")
public class NilInputSource implements InputSource
{
  private static final NilInputSource INSTANCE = new NilInputSource();

  private NilInputSource()
  {
    // Singleton.
  }

  @JsonCreator
  public static NilInputSource instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  public InputSourceReader reader(
      final InputRowSchema inputRowSchema,
      @Nullable final InputFormat inputFormat,
      final File temporaryDirectory
  )
  {
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read()
      {
        return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample()
      {
        return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
      }
    };
  }
}
