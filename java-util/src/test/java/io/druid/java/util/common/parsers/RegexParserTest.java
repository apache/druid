/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.java.util.common.parsers;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class RegexParserTest
{
  @Test
  public void testAWSLog()
  {
    final String pattern = "^([0-9a-f]+) ([\\w.-]+) \\[([\\w\\/: +-]+)\\] ([\\d.]+) ([^\\s]+) ([\\w]+) ([\\w.-]+) ([^\\s\"]+) \"([^\"]*)\" ([\\d-]+) ([\\w-]+) ([\\d-]+) ([\\d-]+) ([\\d-]+) ([\\d-]+) \"(.+)\" \"(.+)\" ([\\w-]+)$";

    final List<String> fieldNames = Arrays.asList(
        "Bucket Owner",
        "Bucket",
        "Time",
        "Remote IP",
        "Requester",
        "Request ID",
        "Operation",
        "Key",
        "Request-URI",
        "HTTP status",
        "Error Code",
        "Bytes Sent",
        "Object Size",
        "Total Time",
        "Turn-Around Time",
        "Referrer",
        "User-Agent",
        "Version ID"
    );

    final Parser<String, Object> parser = new RegexParser(
        pattern,
        Optional.<String>absent(),
        fieldNames
    );
    String data = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - \"GET /mybucket?versioning HTTP/1.1\" 200 - 113 - 7 - \"-\" \"S3Console/0.4\" -";

    final Map<String, Object> parsed = parser.parse(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("Bucket Owner", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be");
    builder.put("Bucket", "mybucket");
    builder.put("Time", "06/Feb/2014:00:00:38 +0000");
    builder.put("Remote IP", "192.0.2.3");
    builder.put("Requester", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be");
    builder.put("Request ID", "3E57427F3EXAMPLE");
    builder.put("Operation", "REST.GET.VERSIONING");
    builder.put("Key", "-");
    builder.put("Request-URI", "GET /mybucket?versioning HTTP/1.1");
    builder.put("HTTP status", "200");
    builder.put("Error Code", "-");
    builder.put("Bytes Sent", "113");
    builder.put("Object Size", "-");
    builder.put("Total Time", "7");
    builder.put("Turn-Around Time", "-");
    builder.put("Referrer", "-");
    builder.put("User-Agent", "S3Console/0.4");
    builder.put("Version ID", "-");

    Assert.assertEquals(
        "result",
        builder.build(),
        parsed
    );
  }

  @Test
  public void testAWSLogWithCrazyUserAgent()
  {
    final String pattern = "^([0-9a-f]+) ([\\w.-]+) \\[([\\w\\/: +-]+)\\] ([\\d.]+) ([^\\s]+) ([\\w]+) ([\\w.-]+) ([^\\s\"]+) \"([^\"]*)\" ([\\d-]+) ([\\w-]+) ([\\d-]+) ([\\d-]+) ([\\d-]+) ([\\d-]+) \"(.+)\" \"(.+)\" ([\\w-]+)$";

    final List<String> fieldNames = Arrays.asList(
        "Bucket Owner",
        "Bucket",
        "Time",
        "Remote IP",
        "Requester",
        "Request ID",
        "Operation",
        "Key",
        "Request-URI",
        "HTTP status",
        "Error Code",
        "Bytes Sent",
        "Object Size",
        "Total Time",
        "Turn-Around Time",
        "Referrer",
        "User-Agent",
        "Version ID"
    );

    final Parser<String, Object> parser = new RegexParser(
        pattern,
        Optional.<String>absent(),
        fieldNames
    );
    String data = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be mybucket [06/Feb/2014:00:01:00 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 7B4A0FABBEXAMPLE REST.GET.VERSIONING - \"GET /mybucket?versioning HTTP/1.1\" 200 - 139 139 27 26 \"-\" \"() { foo;};echo; /bin/bash -c \"expr 299663299665 / 3; echo 333:; uname -a; echo 333:; id;\"\" -";

    final Map<String, Object> parsed = parser.parse(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("Bucket Owner", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be");
    builder.put("Bucket", "mybucket");
    builder.put("Time", "06/Feb/2014:00:01:00 +0000");
    builder.put("Remote IP", "192.0.2.3");
    builder.put("Requester", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be");
    builder.put("Request ID", "7B4A0FABBEXAMPLE");
    builder.put("Operation", "REST.GET.VERSIONING");
    builder.put("Key", "-");
    builder.put("Request-URI", "GET /mybucket?versioning HTTP/1.1");
    builder.put("HTTP status", "200");
    builder.put("Error Code", "-");
    builder.put("Bytes Sent", "139");
    builder.put("Object Size", "139");
    builder.put("Total Time", "27");
    builder.put("Turn-Around Time", "26");
    builder.put("Referrer", "-");
    builder.put(
        "User-Agent",
        "() { foo;};echo; /bin/bash -c \"expr 299663299665 / 3; echo 333:; uname -a; echo 333:; id;\""
    );
    builder.put("Version ID", "-");

    Assert.assertEquals(
        "result",
        builder.build(),
        parsed
    );
  }

  @Test
  public void testMultiVal()
  {
    final String pattern = "^([0-9a-f]+) (.*)";

    final List<String> fieldNames = Arrays.asList(
        "Bucket Owner",
        "Bucket"
    );

    final Parser<String, Object> parser = new RegexParser(
        pattern,
        Optional.of("@"),
        fieldNames
    );
    String data = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be mybucket@mybucket2";

    final Map<String, Object> parsed = parser.parse(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("Bucket Owner", "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be");
    builder.put("Bucket", Lists.newArrayList("mybucket", "mybucket2"));

    Assert.assertEquals(
        "result",
        builder.build(),
        parsed
    );
  }

  @Test
  public void testMultiValWithRegexSplit()
  {
    final String pattern = "(.*)";
    final String listPattern = "[a-f]";

    final Parser<String, Object> parser = new RegexParser(
        pattern,
        Optional.of(listPattern)
    );
    String data = "1a2";

    final Map<String, Object> parsed = parser.parse(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("column_1", Lists.newArrayList("1", "2"));

    Assert.assertEquals(
        "result",
        builder.build(),
        parsed
    );
  }

  @Test(expected = ParseException.class)
  public void testFailure()
  {
    final String pattern = "AAAAA";

    final List<String> fieldNames = Arrays.asList(
        "dummy"
    );

    final Parser<String, Object> parser = new RegexParser(
        pattern,
        Optional.of("@"),
        fieldNames
    );
    String data = "BBBB";

    parser.parse(data);
  }
}
