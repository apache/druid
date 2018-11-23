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

package org.apache.druid.java.util.emitter.core;

public enum BatchingStrategy
{
  ARRAY {

    @Override
    public int batchStartLength()
    {
      return 1;
    }

    @Override
    public int separatorLength()
    {
      return 1;
    }

    @Override
    public int batchEndLength()
    {
      return 2;
    }

    @Override
    public int writeBatchStart(byte[] buffer)
    {
      buffer[0] = '[';
      return batchStartLength();
    }

    @Override
    public int writeMessageSeparator(byte[] buffer, int bufferOffset)
    {
      buffer[bufferOffset] = ',';
      return bufferOffset + separatorLength();
    }

    @Override
    public int writeBatchEnd(byte[] buffer, int bufferOffset)
    {
      buffer[bufferOffset] = ']';
      buffer[bufferOffset + 1] = '\n';
      return bufferOffset + batchEndLength();
    }
  },
  NEWLINES {

    @Override
    public int batchStartLength()
    {
      return 0;
    }

    @Override
    public int separatorLength()
    {
      return 1;
    }

    @Override
    public int batchEndLength()
    {
      return 1;
    }

    @Override
    public int writeBatchStart(byte[] buffer)
    {
      // Write nothing
      return batchStartLength();
    }

    @Override
    public int writeMessageSeparator(byte[] buffer, int bufferOffset)
    {
      buffer[bufferOffset] = '\n';
      return bufferOffset + separatorLength();
    }

    @Override
    public int writeBatchEnd(byte[] buffer, int bufferOffset)
    {
      return writeMessageSeparator(buffer, bufferOffset);
    }
  },
  ONLY_EVENTS {
    @Override
    public int batchStartLength()
    {
      return 0;
    }

    @Override
    public int separatorLength()
    {
      return 0;
    }

    @Override
    public int batchEndLength()
    {
      return 0;
    }

    @Override
    public int writeBatchStart(byte[] buffer)
    {
      return 0;
    }

    @Override
    public int writeMessageSeparator(byte[] buffer, int bufferOffset)
    {
      return bufferOffset;
    }

    @Override
    public int writeBatchEnd(byte[] buffer, int bufferOffset)
    {
      return bufferOffset;
    }
  };

  public abstract int batchStartLength();

  public abstract int separatorLength();

  public abstract int batchEndLength();

  public abstract int writeBatchStart(byte[] buffer);

  public abstract int writeMessageSeparator(byte[] buffer, int bufferOffset);

  public abstract int writeBatchEnd(byte[] buffer, int bufferOffset);
}
