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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.catalog.model.table.export.S3ExportDestination;
import org.apache.druid.catalog.model.table.export.TableDestination;
import org.apache.druid.guice.annotations.UnstableApi;

@UnstableApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = IngestDestination.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = TableDestination.TYPE_KEY, value = TableDestination.class),
    @JsonSubTypes.Type(name = S3ExportDestination.TYPE_KEY, value = S3ExportDestination.class)
})
public interface IngestDestination
{
  String TYPE_PROPERTY = "type";
  String getDestinationName();
}
