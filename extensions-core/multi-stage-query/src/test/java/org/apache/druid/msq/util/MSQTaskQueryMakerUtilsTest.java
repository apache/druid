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

package org.apache.druid.msq.util;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MSQTaskQueryMakerUtilsTest
{
  @Test
  public void testValidateSegmentSortOrder()
  {
    // These are all OK, so validateSegmentSortOrder does nothing.
    MSQTaskQueryMakerUtils.validateSegmentSortOrder(Collections.emptyList(), ImmutableList.of("__time", "a", "b"));
    MSQTaskQueryMakerUtils.validateSegmentSortOrder(ImmutableList.of("__time"), ImmutableList.of("__time", "a", "b"));
    MSQTaskQueryMakerUtils.validateSegmentSortOrder(ImmutableList.of("__time", "b"), ImmutableList.of("__time", "a", "b"));
    MSQTaskQueryMakerUtils.validateSegmentSortOrder(ImmutableList.of("b"), ImmutableList.of("a", "b"));

    // These are not OK.
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> MSQTaskQueryMakerUtils.validateSegmentSortOrder(ImmutableList.of("c"), ImmutableList.of("a", "b"))
    );

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> MSQTaskQueryMakerUtils.validateSegmentSortOrder(
            ImmutableList.of("b", "__time"),
            ImmutableList.of("__time", "a", "b")
        )
    );
  }

  @Test
  public void maskSensitiveJsonKeys()
  {

    String sql1 = "\"REPLACE INTO table "
                  + "OVERWRITE ALL\\n"
                  + "WITH ext AS "
                  + "(SELECT *\\nFROM TABLE(\\n  "
                  + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"},\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n"
                  + "'{\\\"type\\\":\\\"json\\\"}',\\n"
                  + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
                  + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
                  + "FROM ext\\n"
                  + "PARTITIONED BY DAY\"";

    String sql2 = "\"REPLACE INTO table "
                  + "OVERWRITE ALL\\n"
                  + "WITH ext AS "
                  + "(SELECT *\\nFROM TABLE(\\n  "
                  + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\"  :{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"},\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n"
                  + "'{\\\"type\\\":\\\"json\\\"}',\\n"
                  + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
                  + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
                  + "FROM ext\\n"
                  + "PARTITIONED BY DAY\"";

    String sql3 = "\"REPLACE INTO table "
                  + "OVERWRITE ALL\\n"
                  + "WITH ext AS "
                  + "(SELECT *\\nFROM TABLE(\\n  "
                  + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":  {\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"},\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n"
                  + "'{\\\"type\\\":\\\"json\\\"}',\\n"
                  + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
                  + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
                  + "FROM ext\\n"
                  + "PARTITIONED BY DAY\"";

    String sql4 = "\"REPLACE INTO table "
                  + "OVERWRITE ALL\\n"
                  + "WITH ext AS "
                  + "(SELECT *\\nFROM TABLE(\\n  "
                  + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":{  \\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"},\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n"
                  + "'{\\\"type\\\":\\\"json\\\"}',\\n"
                  + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
                  + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
                  + "FROM ext\\n"
                  + "PARTITIONED BY DAY\"";

    String sql5 = "\"REPLACE INTO table "
                  + "OVERWRITE ALL\\n"
                  + "WITH ext AS "
                  + "(SELECT *\\nFROM TABLE(\\n  "
                  + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"  },\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n"
                  + "'{\\\"type\\\":\\\"json\\\"}',\\n"
                  + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
                  + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
                  + "FROM ext\\n"
                  + "PARTITIONED BY DAY\"";

    Assert.assertEquals(
        "\"REPLACE INTO table "
        + "OVERWRITE ALL\\n"
        + "WITH ext AS "
        + "(SELECT *\\nFROM TABLE(\\n  "
        + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":<masked>,\\\"secretAccessKey\\\":<masked>}}',\\n"
        + "'{\\\"type\\\":\\\"json\\\"}',\\n"
        + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
        + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
        + "FROM ext\\n"
        + "PARTITIONED BY DAY\"",
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(sql1)
    );

    Assert.assertEquals(
        "\"REPLACE INTO table "
        + "OVERWRITE ALL\\n"
        + "WITH ext AS "
        + "(SELECT *\\nFROM TABLE(\\n  "
        + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\"  :<masked>,\\\"secretAccessKey\\\":<masked>}}',\\n"
        + "'{\\\"type\\\":\\\"json\\\"}',\\n"
        + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
        + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
        + "FROM ext\\n"
        + "PARTITIONED BY DAY\"",
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(sql2)
    );

    Assert.assertEquals(
        "\"REPLACE INTO table "
        + "OVERWRITE ALL\\n"
        + "WITH ext AS "
        + "(SELECT *\\nFROM TABLE(\\n  "
        + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":  <masked>,\\\"secretAccessKey\\\":<masked>}}',\\n"
        + "'{\\\"type\\\":\\\"json\\\"}',\\n"
        + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
        + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
        + "FROM ext\\n"
        + "PARTITIONED BY DAY\"",
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(sql3)
    );

    Assert.assertEquals(
        "\"REPLACE INTO table "
        + "OVERWRITE ALL\\n"
        + "WITH ext AS "
        + "(SELECT *\\nFROM TABLE(\\n  "
        + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":<masked>,\\\"secretAccessKey\\\":<masked>}}',\\n"
        + "'{\\\"type\\\":\\\"json\\\"}',\\n"
        + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
        + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
        + "FROM ext\\n"
        + "PARTITIONED BY DAY\"",
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(sql4)
    );

    Assert.assertEquals(
        "\"REPLACE INTO table "
        + "OVERWRITE ALL\\n"
        + "WITH ext AS "
        + "(SELECT *\\nFROM TABLE(\\n  "
        + "EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":<masked>,\\\"secretAccessKey\\\":<masked>}}',\\n"
        + "'{\\\"type\\\":\\\"json\\\"}',\\n"
        + "'[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\n"
        + "SELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country "
        + "FROM ext\\n"
        + "PARTITIONED BY DAY\"",
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(sql5)
    );
  }
}
