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

package org.apache.druid.query.lookup.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathParser;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;

import java.util.Objects;

@JsonTypeName("jqJson")
public class JqJsonKafkaLookupDataParser implements KafkaLookupDataParser
{
  private final Parser<String, String> parser;
  private final String keyFieldName;
  private final JSONPathFieldSpec valueJsonSpec;

  @JsonCreator
  public JqJsonKafkaLookupDataParser(
      @JacksonInject @Json ObjectMapper jsonMapper,
      @JsonProperty("keyFieldName") final String keyFieldName,
      @JsonProperty("valueSpec") final JSONPathFieldSpec valueJsonSpec
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyFieldName), "[keyFieldName] cannot be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(valueJsonSpec.getName()), "[valueJsonSpec] cannot be empty");
    this.keyFieldName = keyFieldName;
    this.valueJsonSpec = valueJsonSpec;

    // Copy jsonMapper; don't want to share canonicalization tables, etc., with the global ObjectMapper.
    JSONPathSpec flattenSpec = new JSONPathSpec(
            false,
            ImmutableList.of(
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, keyFieldName, keyFieldName),
                    valueJsonSpec
            )
    );
    this.parser = new UriExtractionNamespace.DelegateParser(
        new JSONPathParser(flattenSpec, jsonMapper.copy()), 
        keyFieldName,
        valueJsonSpec.getName()
    );
  }

  @JsonProperty
  public String getKeyFieldName()
  {
    return this.keyFieldName;
  }

  @JsonProperty
  public JSONPathFieldSpec getValueJsonSpec()
  {
    return this.valueJsonSpec;
  }

  @Override
  public Parser<String, String> getParser()
  {
    return this.parser;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JqJsonKafkaLookupDataParser that = (JqJsonKafkaLookupDataParser) o;
    return Objects.equals(keyFieldName, that.keyFieldName) &&
        Objects.equals(valueJsonSpec, that.valueJsonSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyFieldName, valueJsonSpec);
  }

  @Override
  public String toString()
  {
    return "JSONKafkaFlatDataParser{" +
        "keyFieldName='" + keyFieldName + '\'' +
        ", valueJsonSpec='" + valueJsonSpec + '\'' +
        '}';
  }
}
