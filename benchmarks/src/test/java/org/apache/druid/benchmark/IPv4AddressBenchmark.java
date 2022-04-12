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

package org.apache.druid.benchmark;

import com.google.common.net.InetAddresses;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv4.IPv4Address;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.expression.ExprUtils;
import org.apache.druid.query.expression.IPv4AddressExprUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class IPv4AddressBenchmark
{
  private static final Pattern IPV4_PATTERN = Pattern.compile(
      "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
  );
  // Different representations of IPv4 addresses.
  private static List<String> IPV4_ADDRESS_STRS;
  private static List<IPv4Address> IPV4_ADDRESSES;
  private static List<Inet4Address> INET4_ADDRESSES;
  private static List<Long> IPV4_ADDRESS_LONGS;
  private static List<String> IPV4_SUBNETS;
  @Param({"10", "100", "1000"})
  public int numOfAddresses;

  static boolean isValidAddress(String string)
  {
    return string != null && IPV4_PATTERN.matcher(string).matches();
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(IPv4AddressBenchmark.class.getSimpleName())
        .forks(1)
        .build();
    new Runner(opt).run();
  }

  @Setup
  public void setUp()
  {
    IPV4_ADDRESS_STRS = new ArrayList<>(numOfAddresses);
    IPV4_ADDRESSES = new ArrayList<>(numOfAddresses);
    INET4_ADDRESSES = new ArrayList<>(numOfAddresses);
    IPV4_SUBNETS = new ArrayList<>(numOfAddresses);
    IPV4_ADDRESS_LONGS = new ArrayList<>(numOfAddresses);

    for (int i = 0; i < numOfAddresses; i++) {
      Random r = ThreadLocalRandom.current();
      String genIpAddress = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
      IPAddressString ipAddressString = new IPAddressString(genIpAddress);
      IPAddress prefixBlock = ipAddressString.getAddress().applyPrefixLength(r.nextInt(32)).toPrefixBlock();
      IPV4_ADDRESS_STRS.add(ipAddressString.toString());
      IPV4_SUBNETS.add(prefixBlock.toString());

      IPv4Address iPv4Address = ipAddressString.getAddress().toIPv4();

      IPV4_ADDRESSES.add(iPv4Address);
      INET4_ADDRESSES.add(iPv4Address.toInetAddress());
      IPV4_ADDRESS_LONGS.add(iPv4Address.longValue());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void stringContainsUsingIpAddr()
  {
    for (int i = 0; i < IPV4_ADDRESS_STRS.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESS_STRS.get(i);

      IPAddressString subnetString = new IPAddressString(v4Subnet);
      IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Address);
      if (iPv4Address != null) {
        subnetString.contains(iPv4Address.toAddressString());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void stringContainsUsingSubnetUtils() throws IAE
  {
    for (int i = 0; i < IPV4_ADDRESS_STRS.size(); i++) {
      String v4Subnet = IPV4_SUBNETS.get(i);
      String v4Address = IPV4_ADDRESS_STRS.get(i);

      SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(v4Subnet);
      if (isValidAddress(v4Address)) {
        subnetInfo.isInRange(v4Address);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseLongUsingIpAddr()
  {
    for (Long v4Address : IPV4_ADDRESS_LONGS) {
      IPv4AddressExprUtils.parse(v4Address);

    }
  }

  private Inet4Address parseUsingSubnetUtils(long longValue)
  {
    if (IPv4AddressExprUtils.overflowsUnsignedInt(longValue)) {
      return InetAddresses.fromInteger((int) longValue);
    }
    return null;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseLongUsingSubnetUtils()
  {
    for (Long v4Address : IPV4_ADDRESS_LONGS) {
      parseUsingSubnetUtils(v4Address);
    }
  }

  private long toLongUsingSubnetUtils(Inet4Address address)
  {
    int value = InetAddresses.coerceToInteger(address);
    return Integer.toUnsignedLong(value);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void toLongUsingSubnetUtils()
  {
    for (Inet4Address v4InetAddress : INET4_ADDRESSES) {
      toLongUsingSubnetUtils(v4InetAddress);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void toLongUsingIpAddr()
  {
    for (IPv4Address v4Address : IPV4_ADDRESSES) {
      IPv4AddressExprUtils.toLong(v4Address);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void longContainsUsingSubnetUtils()
  {
    for (int i = 0; i < IPV4_ADDRESS_LONGS.size(); i++) {
      long v4Long = IPV4_ADDRESS_LONGS.get(i);
      String v4Subnet = IPV4_SUBNETS.get(i);
      SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(v4Subnet);

      if (!IPv4AddressExprUtils.overflowsUnsignedInt(v4Long)) {
        subnetInfo.isInRange((int) v4Long);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void longContainsUsingIpAddr()
  {
    for (int i = 0; i < IPV4_ADDRESS_LONGS.size(); i++) {
      long v4Long = IPV4_ADDRESS_LONGS.get(i);
      String v4Subnet = IPV4_SUBNETS.get(i);

      IPv4Address iPv4Address = IPv4AddressExprUtils.parse(v4Long);
      IPAddressString subnetString = new IPAddressString(v4Subnet);
      if (iPv4Address != null) {
        subnetString.contains(iPv4Address.toAddressString());
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseStringUsingIpAddr()
  {
    for (String ipv4Addr : IPV4_ADDRESS_STRS) {
      IPv4AddressExprUtils.parse(ipv4Addr);
    }
  }

  private Inet4Address parseUsingSubnetUtils(String string)
  {
    if (isValidAddress(string)) {
      InetAddress address = InetAddresses.forString(string);
      if (address instanceof Inet4Address) {
        return (Inet4Address) address;
      }
    }
    return null;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseStringUsingSubnetUtils()
  {
    for (String ipv4Addr : IPV4_ADDRESS_STRS) {
      parseUsingSubnetUtils(ipv4Addr);
    }
  }

  private SubnetUtils.SubnetInfo getSubnetInfo(String subnet)
  {
    SubnetUtils subnetUtils;
    try {
      subnetUtils = new SubnetUtils(subnet);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(e, ExprUtils.createErrMsg("getSubnetInfo()", " arg has an invalid format: " + subnet));
    }
    subnetUtils.setInclusiveHostCount(true);  // make network and broadcast addresses match
    return subnetUtils.getInfo();
  }
}
