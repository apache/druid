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

package org.apache.druid.io;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IOE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;

/**
 * An {@link OutputStream} that limits how many bytes can be written. Throws {@link IOException} if the limit
 * is exceeded.
 */
public class LimitedOutputStream extends OutputStream
{
  private final OutputStream out;
  private final long limit;
  private final Function<Long, String> exceptionMessageFn;
  long written;

  /**
   * Create a bytes-limited output stream.
   *
   * @param out                output stream to wrap
   * @param limit              bytes limit
   * @param exceptionMessageFn function for generating an exception message for an {@link IOException}, given the limit.
   */
  public LimitedOutputStream(OutputStream out, long limit, Function<Long, String> exceptionMessageFn)
  {
    this.out = out;
    this.limit = limit;
    this.exceptionMessageFn = exceptionMessageFn;

    if (limit < 0) {
      throw DruidException.defensive("Limit[%s] must be greater than or equal to zero", limit);
    }
  }

  @Override
  public void write(int b) throws IOException
  {
    plus(1);
    out.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException
  {
    plus(b.length);
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    plus(len);
    out.write(b, off, len);
  }

  @Override
  public void flush() throws IOException
  {
    out.flush();
  }

  @Override
  public void close() throws IOException
  {
    out.close();
  }

  private void plus(final int n) throws IOException
  {
    written += n;
    if (written > limit) {
      throw new IOE(exceptionMessageFn.apply(limit));
    }
  }
}
