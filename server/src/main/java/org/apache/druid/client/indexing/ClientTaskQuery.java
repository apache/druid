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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * org.apache.druid.indexing.common.task.Task representations for clients. The magic conversion happens right
 * at the moment of making a REST query: {@link HttpIndexingServiceClient#runTask} serializes ClientTaskQuery
 * objects and org.apache.druid.indexing.overlord.http.OverlordResource.taskPost() deserializes
 * org.apache.druid.indexing.common.task.Task objects from the same bytes. Therefore JSON serialization fields of
 * ClientTaskQuery objects must match with those of the corresponding org.apache.druid.indexing.common.task.Task
 * objects.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "kill", value = ClientKillUnusedSegmentsTaskQuery.class),
    @Type(name = "compact", value = ClientCompactionTaskQuery.class)
})
public interface ClientTaskQuery
{
  String getType();

  String getDataSource();
}
