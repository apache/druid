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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.server.security.Resource;

import java.util.Optional;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DataSourceMSQDestination.TYPE, value = DataSourceMSQDestination.class),
    @JsonSubTypes.Type(name = TaskReportMSQDestination.TYPE, value = TaskReportMSQDestination.class),
    @JsonSubTypes.Type(name = ExportMSQDestination.TYPE, value = ExportMSQDestination.class),
    @JsonSubTypes.Type(name = DurableStorageMSQDestination.TYPE, value = DurableStorageMSQDestination.class)
})
public interface MSQDestination
{
  ShuffleSpecFactory getShuffleSpecFactory(int targetSize);

  Optional<Resource> getDestinationResource();
}
