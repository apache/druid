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

package org.apache.druid.data.input.parquet;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.data.input.parquet.avro.ParquetAvroHadoopInputRowParser;
import org.apache.druid.data.input.parquet.simple.ParquetHadoopInputRowParser;
import org.apache.druid.data.input.parquet.simple.ParquetParseSpec;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class ParquetExtensionsModule implements DruidModule
{
  public static final String PARQUET_SIMPLE_INPUT_PARSER_TYPE = "parquet";
  public static final String PARQUET_SIMPLE_PARSE_SPEC_TYPE = "parquet";
  public static final String PARQUET_AVRO_INPUT_PARSER_TYPE = "parquet-avro";
  public static final String PARQUET_AVRO_PARSE_SPEC_TYPE = "avro";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ParquetInputRowParserModule")
            .registerSubtypes(
                new NamedType(ParquetAvroHadoopInputRowParser.class, PARQUET_AVRO_INPUT_PARSER_TYPE),
                new NamedType(ParquetHadoopInputRowParser.class, PARQUET_SIMPLE_INPUT_PARSER_TYPE),
                new NamedType(ParquetParseSpec.class, PARQUET_SIMPLE_INPUT_PARSER_TYPE)
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
