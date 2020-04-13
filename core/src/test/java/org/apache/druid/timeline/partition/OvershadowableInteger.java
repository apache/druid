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

package org.apache.druid.timeline.partition;

import org.apache.druid.timeline.Overshadowable;

import java.util.Objects;

public class OvershadowableInteger implements Overshadowable<OvershadowableInteger>
{
  private final String majorVersion;
  private final int partitionNum;
  private final int val;
  private final int startRootPartitionId;
  private final int endRootPartitionId;
  private final short minorVersion;
  private final short atomicUpdateGroupSize;

  public OvershadowableInteger(int val)
  {
    this("", 0, val);
  }

  public OvershadowableInteger(String majorVersion, int partitionNum, int val)
  {
    this(majorVersion, partitionNum, val, partitionNum, partitionNum + 1, 0, 1);
  }

  public OvershadowableInteger(
      String majorVersion,
      int partitionNum,
      int val,
      int startRootPartitionId,
      int endRootPartitionId,
      int minorVersion,
      int atomicUpdateGroupSize
  )
  {
    this.majorVersion = majorVersion;
    this.partitionNum = partitionNum;
    this.val = val;
    this.startRootPartitionId = startRootPartitionId;
    this.endRootPartitionId = endRootPartitionId;
    this.minorVersion = (short) minorVersion;
    this.atomicUpdateGroupSize = (short) atomicUpdateGroupSize;
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
    OvershadowableInteger that = (OvershadowableInteger) o;
    return partitionNum == that.partitionNum &&
           val == that.val &&
           startRootPartitionId == that.startRootPartitionId &&
           endRootPartitionId == that.endRootPartitionId &&
           minorVersion == that.minorVersion &&
           atomicUpdateGroupSize == that.atomicUpdateGroupSize &&
           Objects.equals(majorVersion, that.majorVersion);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        majorVersion,
        partitionNum,
        val,
        startRootPartitionId,
        endRootPartitionId,
        minorVersion,
        atomicUpdateGroupSize
    );
  }

  @Override
  public String toString()
  {
    return "OvershadowableInteger{" +
           "majorVersion='" + majorVersion + '\'' +
           ", partitionNum=" + partitionNum +
           ", val=" + val +
           ", startRootPartitionId=" + startRootPartitionId +
           ", endRootPartitionId=" + endRootPartitionId +
           ", minorVersion=" + minorVersion +
           ", atomicUpdateGroupSize=" + atomicUpdateGroupSize +
           '}';
  }

  @Override
  public int getStartRootPartitionId()
  {
    return startRootPartitionId;
  }

  @Override
  public int getEndRootPartitionId()
  {
    return endRootPartitionId;
  }

  @Override
  public String getVersion()
  {
    return majorVersion;
  }

  @Override
  public short getMinorVersion()
  {
    return minorVersion;
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return atomicUpdateGroupSize;
  }
}
