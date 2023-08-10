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

package org.apache.druid.storage.azure.output;

import java.util.Objects;

/**
 * Represents a chunk of the Azure blob
 */
public class AzureInputRange
{

  /**
   * Starting location in the blob stream
   */
  private final long start;

  /**
   * Size of the blob stream that this object represents
   */
  private final long size;

  /**
   * Container where the blob resides
   */
  private final String container;

  /**
   * Absolute path of the blob
   */
  private final String path;

  public AzureInputRange(long start, long size, String container, String path)
  {
    this.start = start;
    this.size = size;
    this.container = container;
    this.path = path;
  }

  public long getStart()
  {
    return start;
  }

  public long getSize()
  {
    return size;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPath()
  {
    return path;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AzureInputRange that = (AzureInputRange) o;
    return start == that.start
           && size == that.size
           && Objects.equals(container, that.container)
           && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, size, container, path);
  }
}
