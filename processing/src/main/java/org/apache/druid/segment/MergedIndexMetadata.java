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
package org.apache.druid.segment;

import java.io.File;
import java.util.List;

public class MergedIndexMetadata
{
  private final File file;
  private final List<String> dimensions;

  public MergedIndexMetadata(File file, List<String> dimensions)
  {
    this.file = file;
    this.dimensions = dimensions;
  }

  public File getFile()
  {
    return file;
  }

  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "MergedIndexMetadata{" +
           "file=" + file +
           ", dimensions=" + dimensions +
           '}';
  }
}
