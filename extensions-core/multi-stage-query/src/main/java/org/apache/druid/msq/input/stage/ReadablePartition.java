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

package org.apache.druid.msq.input.stage;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

/**
 * Represents an output partition associated with a particular stage. Each partition is readable from one or
 * more workers.
 */
public class ReadablePartition
{
  private final int stageNumber;
  private final IntSortedSet workerNumbers;
  private final int partitionNumber;

  private ReadablePartition(final int stageNumber, final IntSortedSet workerNumbers, final int partitionNumber)
  {
    this.stageNumber = stageNumber;
    this.workerNumbers = workerNumbers;
    this.partitionNumber = partitionNumber;

    if (workerNumbers.isEmpty()) {
      throw new IAE("Cannot have empty worker set");
    }
  }

  /**
   * Returns an output partition that is striped across {@code numWorkers} workers.
   */
  public static ReadablePartition striped(final int stageNumber, final int numWorkers, final int partitionNumber)
  {
    final IntAVLTreeSet workerNumbers = new IntAVLTreeSet();
    for (int i = 0; i < numWorkers; i++) {
      workerNumbers.add(i);
    }

    return new ReadablePartition(stageNumber, workerNumbers, partitionNumber);
  }

  /**
   * Returns an output partition that has been collected onto a single worker.
   */
  public static ReadablePartition collected(final int stageNumber, final int workerNumber, final int partitionNumber)
  {
    final IntAVLTreeSet workerNumbers = new IntAVLTreeSet();
    workerNumbers.add(workerNumber);

    return new ReadablePartition(stageNumber, workerNumbers, partitionNumber);
  }

  public int getStageNumber()
  {
    return stageNumber;
  }

  public IntSortedSet getWorkerNumbers()
  {
    return workerNumbers;
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
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
    ReadablePartition that = (ReadablePartition) o;
    return stageNumber == that.stageNumber
           && partitionNumber == that.partitionNumber
           && Objects.equals(workerNumbers, that.workerNumbers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, workerNumbers, partitionNumber);
  }

  @Override
  public String toString()
  {
    return "ReadablePartition{" +
           "stageNumber=" + stageNumber +
           ", workerNumbers=" + workerNumbers +
           ", partitionNumber=" + partitionNumber +
           '}';
  }
}
