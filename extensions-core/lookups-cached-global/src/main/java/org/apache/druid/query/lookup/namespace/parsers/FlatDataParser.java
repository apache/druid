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

package org.apache.druid.query.lookup.namespace.parsers;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.parsers.Parser;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class),
    @JsonSubTypes.Type(name = "tsv", value = TSVFlatDataParser.class),
    @JsonSubTypes.Type(name = "customJson", value = JSONFlatDataParser.class),
    @JsonSubTypes.Type(name = "simpleJson", value = ObjectMapperFlatDataParser.class)
})
public interface FlatDataParser
{
  Parser<String, String> getParser();
}
