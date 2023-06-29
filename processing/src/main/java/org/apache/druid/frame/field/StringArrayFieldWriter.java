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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

/**
 * Like {@link StringFieldWriter}, but reads arrays from a {@link ColumnValueSelector} instead of reading from
 * a {@link org.apache.druid.segment.DimensionSelector}.
 *
 * See {@link StringFieldReader} for format details.
 */
public class StringArrayFieldWriter implements FieldWriter
{
  private final ColumnValueSelector<List<String>> selector;

  public StringArrayFieldWriter(final ColumnValueSelector<List<String>> selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    return StringFieldWriter.writeUtf8ByteBuffers(
        memory,
        position,
        maxSize,
        FrameWriterUtils.getUtf8ByteBuffersFromStringArraySelector(selector)
    );
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
