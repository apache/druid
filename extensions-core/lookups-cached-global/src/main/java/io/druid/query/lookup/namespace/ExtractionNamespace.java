/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "jdbc", value = JdbcExtractionNamespace.class),
    @JsonSubTypes.Type(name = "uri", value = UriExtractionNamespace.class),
    @JsonSubTypes.Type(name = StaticMapExtractionNamespace.TYPE_NAME, value = StaticMapExtractionNamespace.class)
})
/**
 * The ExtractionNamespace is a simple object for extracting namespaceLookup values from a source of data.
 * It is expected to have an accompanying ExtractionNamespaceFunctionFactory which handles the actual
 * extraction implementation.
 */
public interface ExtractionNamespace
{
  long getPollMs();
}
