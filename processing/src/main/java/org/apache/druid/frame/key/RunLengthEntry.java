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

package org.apache.druid.frame.key;

import java.util.Objects;

/**
 * Information about a continguous run of keys, that has the same sorting order
 */
public class RunLengthEntry
{
  private final boolean byteComparable;
  private final KeyOrder order;
  private final int runLength;

  RunLengthEntry(final boolean byteComparable, final KeyOrder order, final int runLength)
  {
    this.byteComparable = byteComparable;
    this.order = order;
    this.runLength = runLength;
  }

  public boolean isByteComparable()
  {
    return byteComparable;
  }

  public int getRunLength()
  {
    return runLength;
  }

  public KeyOrder getOrder()
  {
    return order;
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
    RunLengthEntry that = (RunLengthEntry) o;
    return byteComparable == that.byteComparable && runLength == that.runLength && order == that.order;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(byteComparable, order, runLength);
  }

  @Override
  public String toString()
  {
    return "RunLengthEntry{" +
           "byteComparable=" + byteComparable +
           ", order=" + order +
           ", runLength=" + runLength +
           '}';
  }
}
