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

import java.util.Objects;

@JsonTypeName("customJson")
public class KafkaJsonFlatDataParser implements KafkaLookupDataParser
{
  private final Parser<String, String> parser;
  private final String keyFieldName;
  private final String valueFieldName;

  @JsonCreator
  public KafkaJsonFlatDataParser(
      @JacksonInject @Json ObjectMapper jsonMapper,
      @JsonProperty("keyFieldName") final String keyFieldName,
      @JsonProperty("valueFieldName") final String valueFieldName
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(keyFieldName), "[keyFieldName] cannot be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(valueFieldName), "[valueFieldName] cannot be empty");
    this.keyFieldName = keyFieldName;
    this.valueFieldName = valueFieldName;

    // Copy jsonMapper; don't want to share canonicalization tables, etc., with the global ObjectMapper.
    this.parser = new KafkaDelegateParser(
        new JSONPathParser(
            new JSONPathSpec(
                false,
                ImmutableList.of(
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, keyFieldName, keyFieldName),
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, valueFieldName, valueFieldName)
                )
            ),
            jsonMapper.copy()
        ),
        keyFieldName,
        valueFieldName
    );
  }

  @JsonProperty
  public String getKeyFieldName()
  {
    return this.keyFieldName;
  }

  @JsonProperty
  public String getValueFieldName()
  {
    return this.valueFieldName;
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
    final KafkaJsonFlatDataParser that = (KafkaJsonFlatDataParser) o;
    return Objects.equals(keyFieldName, that.keyFieldName) &&
        Objects.equals(valueFieldName, that.valueFieldName);
  }

  @Override
  public String toString()
  {
    return "KafkaJsonFlatDataParser{" +
        "keyFieldName='" + keyFieldName + '\'' +
        ", valueFieldName='" + valueFieldName + '\'' +
        '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyFieldName, valueFieldName);
  }
}
