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

package org.apache.druid.indexer.hbase.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.Parser;

public class HBaseParseSpec extends ParseSpec
{

  private final HBaseRowSpec hbaseRowSpec;

  @JsonCreator
  protected HBaseParseSpec(@JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("hbaseRowSpec") HBaseRowSpec hbaseRowSpec)
  {
    super(timestampSpec, dimensionsSpec);
    this.hbaseRowSpec = hbaseRowSpec;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return super.makeParser();
  }

  @JsonProperty("hbaseRowSpec")
  public HBaseRowSpec getHbaseRowSpec()
  {
    return hbaseRowSpec;
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new HBaseParseSpec(getTimestampSpec(), getDimensionsSpec(), hbaseRowSpec);
  }

}
