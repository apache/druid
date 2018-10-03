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

package org.apache.druid.server;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.router.ConsistentHasher;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ConsistentHasherTest
{
  private static HashFunction TEST_HASH_FN = Hashing.murmur3_128();
  private static int NUM_ITERATIONS = 10000;
  private static final Logger log = new Logger(ConsistentHasherTest.class);

  @Test
  public void testBasic()
  {
    ConsistentHasher hasher = new ConsistentHasher(TEST_HASH_FN);
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:1");
    nodes.add("localhost:2");
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");

    Map<String, String> uuidServerMap = new HashMap<>();

    hasher.updateKeys(nodes);

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.findKey(StringUtils.toUtf8(objectId.toString()));
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    // check that the same UUIDs hash to the same servers on subsequent hashStr() calls
    for (int i = 0; i < 2; i++) {
      for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
        String targetServer = hasher.findKey(StringUtils.toUtf8(entry.getKey()));
        Assert.assertEquals(entry.getValue(), targetServer);
      }
    }
  }

  @Test
  public void testAddNode()
  {
    ConsistentHasher hasher = new ConsistentHasher(TEST_HASH_FN);
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:1");
    nodes.add("localhost:2");
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");
    hasher.updateKeys(nodes);

    Map<String, String> uuidServerMap = new HashMap<>();

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.findKey(StringUtils.toUtf8(objectId.toString()));
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    nodes.add("localhost:6");
    hasher.updateKeys(nodes);

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher.findKey(StringUtils.toUtf8(entry.getKey()));
      if (entry.getValue().equals(targetServer)) {
        same += 1;
      } else {
        diff += 1;
      }
    }
    log.info(StringUtils.format("testAddNode Total: %s, Same: %s, Diff: %s", NUM_ITERATIONS, same, diff));

    // ~1/6 of the entries should change, check that less than 1/5 of the entries hash differently
    double diffRatio = ((double) diff) / NUM_ITERATIONS;
    Assert.assertTrue(diffRatio < 0.20);
  }

  @Test
  public void testRemoveNode()
  {
    ConsistentHasher hasher = new ConsistentHasher(TEST_HASH_FN);
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:1");
    nodes.add("localhost:2");
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");
    hasher.updateKeys(nodes);

    Map<String, String> uuidServerMap = new HashMap<>();

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.findKey(StringUtils.toUtf8(objectId.toString()));
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    nodes.remove("localhost:3");
    hasher.updateKeys(nodes);

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher.findKey(StringUtils.toUtf8(entry.getKey()));
      if (entry.getValue().equals(targetServer)) {
        same += 1;
      } else {
        diff += 1;
      }
    }
    log.info(StringUtils.format("testRemoveNode Total: %s, Same: %s, Diff: %s", NUM_ITERATIONS, same, diff));

    // ~1/5 of the entries should change, check that less than 1/4 of the entries hash differently
    double diffRatio = ((double) diff) / NUM_ITERATIONS;
    Assert.assertTrue(diffRatio < 0.25);
  }

  @Test
  public void testInconsistentView1()
  {
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:1");
    nodes.add("localhost:2");
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");

    Set<String> nodes2 = new HashSet<>();
    nodes2.add("localhost:1");
    nodes2.add("localhost:3");
    nodes2.add("localhost:4");
    nodes2.add("localhost:5");

    testInconsistentViewHelper("testInconsistentView1", nodes, nodes2, 0.33);
  }

  @Test
  public void testInconsistentView2()
  {
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:1");
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");

    Set<String> nodes2 = new HashSet<>();
    nodes2.add("localhost:1");
    nodes2.add("localhost:2");
    nodes2.add("localhost:4");
    nodes2.add("localhost:5");

    testInconsistentViewHelper("testInconsistentView2", nodes, nodes2, 0.50);
  }

  @Test
  public void testInconsistentView3()
  {
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:3");
    nodes.add("localhost:4");
    nodes.add("localhost:5");

    Set<String> nodes2 = new HashSet<>();
    nodes2.add("localhost:1");
    nodes2.add("localhost:4");
    nodes2.add("localhost:5");

    testInconsistentViewHelper("testInconsistentView3", nodes, nodes2, 0.66);
  }

  @Test
  public void testInconsistentView4()
  {
    Set<String> nodes = new HashSet<>();
    nodes.add("localhost:2");
    nodes.add("localhost:5");

    Set<String> nodes2 = new HashSet<>();
    nodes2.add("localhost:1");
    nodes2.add("localhost:4");
    nodes2.add("localhost:5");

    testInconsistentViewHelper("testInconsistentView4", nodes, nodes2, 0.95);
  }

  public void testInconsistentViewHelper(
      String testName,
      Set<String> nodes,
      Set<String> nodes2,
      double expectedDiffRatio
  )
  {
    ConsistentHasher hasher = new ConsistentHasher(TEST_HASH_FN);
    hasher.updateKeys(nodes);

    Map<String, String> uuidServerMap = new HashMap<>();
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UUID objectId = UUID.randomUUID();
      String targetServer = hasher.findKey(StringUtils.toUtf8(objectId.toString()));
      uuidServerMap.put(objectId.toString(), targetServer);
    }

    ConsistentHasher hasher2 = new ConsistentHasher(TEST_HASH_FN);
    hasher2.updateKeys(nodes2);

    Map<String, String> uuidServerMap2 = new HashMap<>();
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String targetServer = hasher2.findKey(StringUtils.toUtf8(entry.getKey()));
      uuidServerMap2.put(entry.getKey(), targetServer);
    }

    int same = 0;
    int diff = 0;
    for (Map.Entry<String, String> entry : uuidServerMap.entrySet()) {
      String otherServer = uuidServerMap2.get(entry.getKey());
      if (entry.getValue().equals(otherServer)) {
        same += 1;
      } else {
        diff += 1;
      }
    }
    double actualDiffRatio = ((double) diff) / NUM_ITERATIONS;

    log.info(StringUtils.format("%s Total: %s, Same: %s, Diff: %s", testName, NUM_ITERATIONS, same, diff));
    log.info("Expected diff ratio: %s, Actual diff ratio: %s", expectedDiffRatio, actualDiffRatio);

    Assert.assertTrue(actualDiffRatio <= expectedDiffRatio);
  }
}
