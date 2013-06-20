package druid.examples.twitter;
/*
 * Copyright 2007 Yusuke Yamamoto
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

final class StreamingGZIPInputStream extends GZIPInputStream
{

  private final InputStream wrapped;

  public StreamingGZIPInputStream(InputStream is) throws IOException
  {
    super(is);
    wrapped = is;
  }

  /**
   * Overrides behavior of GZIPInputStream which assumes we have all the data available
   * which is not true for streaming. We instead rely on the underlying stream to tell us
   * how much data is available.
   * <p/>
   * Programs should not count on this method to return the actual number
   * of bytes that could be read without blocking.
   *
   * @return - whatever the wrapped InputStream returns
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public int available() throws IOException {
    return wrapped.available();
  }
}
