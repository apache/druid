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

package org.apache.druid.sql.avatica;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.List;

public class DruidPrettyPrinterTest
{
  static class TwoInlined
  {
    @JsonProperty
    final LimitSpec limitSpec = NoopLimitSpec.INSTANCE;
    @JsonProperty
    final LimitSpec limitSpec2 = NoopLimitSpec.INSTANCE;
  }

  @Test
  public void prettyPrintTwoInlined() throws Exception
  {
    ObjectMapper om = new DefaultObjectMapper();
    ObjectWriter w = om.writer(new DruidPrettyPrinter());

    String s = w.writeValueAsString(new TwoInlined());
    System.out.println(s);

  }


  static class SampleClass
  {
    @JsonProperty
    final RowSignature rowSignature;
    @JsonProperty
    final LimitSpec limitSpec;
    @JsonProperty
    final LimitSpec limitSpec2;

    public SampleClass(RowSignature rs, LimitSpec ls)
    {
      this.rowSignature = rs;
      this.limitSpec = ls;
      this.limitSpec2 = ls;
    }
  }

@Test
  public void prettyPrint1() throws Exception
  {

    ObjectMapper om = new DefaultObjectMapper();
    ObjectWriter w = om.writer(new DruidPrettyPrinter());

    final RowSignature.Builder builder = RowSignature.builder()
        .add("s", ColumnType.STRING)
        .add("d", ColumnType.DOUBLE)
        .add("d1", ColumnType.LONG);

    List<org.apache.druid.segment.column.ColumnSignature> cs = builder.build().asColumnSignatures();

    String s = w.writeValueAsString(new SampleClass(builder.build(), NoopLimitSpec.INSTANCE));
    System.out.println(s);

  }

}
