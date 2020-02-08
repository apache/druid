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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.InputRow;
import org.joda.time.Interval;

/**
 * This sequence name function should be used for the linear partitioning. Since the segments are created as needed,
 * this function uses a single sequence name.
 *
 * @see org.apache.druid.indexer.partitions.SecondaryPartitionType
 */
public class LinearlyPartitionedSequenceNameFunction implements SequenceNameFunction
{
  private final String taskId;

  LinearlyPartitionedSequenceNameFunction(String taskId)
  {
    this.taskId = taskId;
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return taskId;
  }
}
