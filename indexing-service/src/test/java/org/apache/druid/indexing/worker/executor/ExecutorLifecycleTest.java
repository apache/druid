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

package org.apache.druid.indexing.worker.executor;

import org.junit.Assert;
import org.junit.Test;

public class ExecutorLifecycleTest
{

  @Test
  public void maskSensitiveJsonKeys()
  {
    String json = "\"REPLACE INTO table OVERWRITE ALL\\nWITH ext AS (SELECT *\\nFROM TABLE(\\n  EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"},\\\"secretAccessKey\\\":{\\\"type\\\":\\\"default\\\",\\\"password\\\":\\\"secret_pass\\\"}}}',\\n    '{\\\"type\\\":\\\"json\\\"}',\\n    '[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\nSELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country FROM ext\\nPARTITIONED BY DAY\"";
    String maskedJson = ExecutorLifecycle.maskSensitiveJsonKeys(json);
    Assert.assertEquals(
        "\"REPLACE INTO table OVERWRITE ALL\\nWITH ext AS (SELECT *\\nFROM TABLE(\\n  EXTERN(\\n    '{\\\"type\\\":\\\"s3\\\",\\\"prefixes\\\":[\\\"s3://prefix\\\"],\\\"properties\\\":{\\\"accessKeyId\\\":<masked>,\\\"secretAccessKey\\\":<masked>}}',\\n    '{\\\"type\\\":\\\"json\\\"}',\\n    '[{\\\"name\\\":\\\"time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]'\\n  )\\n))\\nSELECT\\n  TIME_PARSE(\\\"time\\\") AS __time,\\n  name,\\n  country FROM ext\\nPARTITIONED BY DAY\"",
        maskedJson
    );
  }
}
