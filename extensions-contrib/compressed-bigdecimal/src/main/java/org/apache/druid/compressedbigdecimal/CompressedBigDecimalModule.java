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

package org.apache.druid.compressedbigdecimal;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

/**
 * Druid module for BigDecimal complex metrics and aggregator.
 */
public class CompressedBigDecimalModule implements DruidModule
{
  public static final String COMPRESSED_BIG_DECIMAL = "compressedBigDecimal";
  public static final String COMPRESSED_BIG_DECIMAL_SUM = "compressedBigDecimalSum";
  public static final String COMPRESSED_BIG_DECIMAL_MAX = "compressedBigDecimalMax";
  public static final String COMPRESSED_BIG_DECIMAL_MIN = "compressedBigDecimalMin";

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, CompressedBigDecimalSumSqlAggregator.class);
    SqlBindings.addAggregator(binder, CompressedBigDecimalMaxSqlAggregator.class);
    SqlBindings.addAggregator(binder, CompressedBigDecimalMinSqlAggregator.class);
  }

  public static void registerSerde()
  {
    if (ComplexMetrics.getSerdeForType(COMPRESSED_BIG_DECIMAL) == null) {
      ComplexMetrics.registerSerde(COMPRESSED_BIG_DECIMAL, new CompressedBigDecimalMetricSerde());
    }
  }

  @Override
  public List<Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("CompressedBigDecimalModule")
            .registerSubtypes(new NamedType(CompressedBigDecimalSumAggregatorFactory.class, COMPRESSED_BIG_DECIMAL_SUM))
            .registerSubtypes(new NamedType(CompressedBigDecimalMaxAggregatorFactory.class, COMPRESSED_BIG_DECIMAL_MAX))
            .registerSubtypes(new NamedType(CompressedBigDecimalMinAggregatorFactory.class, COMPRESSED_BIG_DECIMAL_MIN))
            .addSerializer(CompressedBigDecimal.class, new CompressedBigDecimalJsonSerializer())
    );
  }
}
