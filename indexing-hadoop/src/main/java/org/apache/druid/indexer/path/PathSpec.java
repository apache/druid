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

package org.apache.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "granular_unprocessed", value = GranularUnprocessedPathSpec.class),
    @JsonSubTypes.Type(name = "granularity", value = GranularityPathSpec.class),
    @JsonSubTypes.Type(name = "static", value = StaticPathSpec.class),
    @JsonSubTypes.Type(name = DatasourcePathSpec.TYPE, value = DatasourcePathSpec.class),
    @JsonSubTypes.Type(name = "multi", value = MultiplePathSpec.class)
})
public interface PathSpec
{
  Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException;
}
