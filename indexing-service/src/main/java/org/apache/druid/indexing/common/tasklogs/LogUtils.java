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

package org.apache.druid.indexing.common.tasklogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

public class LogUtils
{
  /**
   * Open a stream to a file.
   *
   * @param offset If zero, stream the entire log. If positive, read from this byte position onwards. If negative,
   *               read this many bytes from the end of the file.
   *
   * @return input supplier for this log, if available from this provider
   */
  public static InputStream streamFile(final File file, final long offset) throws IOException
  {
    final RandomAccessFile raf = new RandomAccessFile(file, "r");
    final long rafLength = raf.length();
    if (offset > 0) {
      raf.seek(offset);
    } else if (offset < 0 && offset < rafLength) {
      raf.seek(Math.max(0, rafLength + offset));
    }
    return Channels.newInputStream(raf.getChannel());
  }
}
