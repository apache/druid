/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.collect.Maps;
import io.druid.java.util.common.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 */
class IOPeonForTesting implements IOPeon
{
  Map<String, ByteArrayOutputStream> outStreams = Maps.newHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    ByteArrayOutputStream stream = outStreams.get(filename);

    if (stream == null) {
      stream = new ByteArrayOutputStream();
      outStreams.put(filename, stream);
    }

    return stream;
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    ByteArrayOutputStream outStream = outStreams.get(filename);

    if (outStream == null) {
      throw new FileNotFoundException(StringUtils.format("unknown file[%s]", filename));
    }

    return new ByteArrayInputStream(outStream.toByteArray());
  }

  @Override
  public void close() throws IOException
  {
    outStreams.clear();
  }

  @Override
  public File getFile(String filename)
  {
    return null;
  }
}
