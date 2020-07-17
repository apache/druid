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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * Each sub task of {@link ParallelIndexSupervisorTask} reports the result of indexing using this class.
 */
@JsonTypeInfo(use = Id.NAME, property = "type", defaultImpl = PushedSegmentsReport.class)
@JsonSubTypes(value = {
    @Type(name = PushedSegmentsReport.TYPE, value = PushedSegmentsReport.class),
    @Type(name = DimensionDistributionReport.TYPE, value = DimensionDistributionReport.class),
    @Type(name = GeneratedPartitionsMetadataReport.TYPE, value = GeneratedPartitionsMetadataReport.class)
})
public interface SubTaskReport
{
  String getTaskId();
}
