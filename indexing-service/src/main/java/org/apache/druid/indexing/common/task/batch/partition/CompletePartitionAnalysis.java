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

package org.apache.druid.indexing.common.task.batch.partition;

import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * This interface represents the PartitionAnalysis that has the complete picture of secondary partitions to create.
 * This type of PartitionAnalysis can be used for the hash or range partitioning in which all secondary partitions
 * should be determined when the analysis is done.
 */
public interface CompletePartitionAnalysis<T, P extends PartitionsSpec> extends PartitionAnalysis<T, P>
{
  Map<Interval, List<BucketNumberedShardSpec<?>>> createBuckets(TaskToolbox toolbox);
}
