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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "jdbc", value = JDBCExtractionNamespace.class),
    @JsonSubTypes.Type(name = "uri", value = URIExtractionNamespace.class)
})
/**
 * The ExtractionNamespace is a simple object for extracting namespaceLookup values from a source of data.
 * It is expected to have an accompanying ExtractionNamespaceFunctionFactory which handles the actual
 * extraction implementation.
 */
public interface ExtractionNamespace
{
  /**
   * This is expected to return the namespace name. As an additional requirement, the implementation MUST supply a
   * "namespace" field in the json representing the object which is equal to the return of this function
   * @return The name of the namespace
   */
  String getNamespace();
  long getPollMs();
}
