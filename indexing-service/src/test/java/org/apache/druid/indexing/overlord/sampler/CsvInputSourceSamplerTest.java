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

package org.apache.druid.indexing.overlord.sampler;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse.SamplerResponseRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CsvInputSourceSamplerTest
{
  @Test
  public void testCSVColumnAllNull()
  {
    final TimestampSpec timestampSpec = new TimestampSpec(null, null, DateTimes.of("1970"));
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final DataSchema dataSchema = new DataSchema(
        "sampler",
        timestampSpec,
        dimensionsSpec,
        null,
        null,
        null
    );

    final List<String> strCsvRows = ImmutableList.of(
        "FirstName,LastName,Number,Gender",
        "J,G,,Male",
        "Kobe,Bryant,,Male",
        "Lisa, Krystal,,Female",
        "Michael,Jackson,,Male"
    );
    final InputSource inputSource = new InlineInputSource(String.join("\n", strCsvRows));
    final InputFormat inputFormat = new CsvInputFormat(null, null, null, true, 0);
    final InputSourceSampler inputSourceSampler = new InputSourceSampler();

    final SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        inputFormat,
        dataSchema,
        null
    );

    Assert.assertEquals(4, response.getNumRowsRead());
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(
        new SamplerResponseRow(
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("FirstName", "J")
                .put("LastName", "G")
                .put("Gender", "Male")
                .build(),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("Number", null)
                .put("FirstName", "J")
                .put("LastName", "G")
                .put("Gender", "Male")
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    Assert.assertEquals(
        new SamplerResponseRow(
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("FirstName", "Kobe")
                .put("LastName", "Bryant")
                .put("Gender", "Male")
                .build(),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("__time", 0L)
                .put("FirstName", "Kobe")
                .put("LastName", "Bryant")
                .put("Gender", "Male")
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    Assert.assertEquals(
        new SamplerResponseRow(
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("FirstName", "Lisa")
                .put("LastName", " Krystal")
                .put("Gender", "Female")
                .build(),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("__time", 0L)
                .put("FirstName", "Lisa")
                .put("LastName", " Krystal")
                .put("Gender", "Female")
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    Assert.assertEquals(
        new SamplerResponseRow(
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("Number", null)
                .put("FirstName", "Michael")
                .put("LastName", "Jackson")
                .put("Gender", "Male")
                .build(),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("Number", null)
                .put("FirstName", "Michael")
                .put("LastName", "Jackson")
                .put("Gender", "Male")
                .build(),
            null,
            null
        ),
        data.get(3)
    );
  }
}
