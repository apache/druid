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

package org.apache.druid.java.util.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class SysMonitorOshi extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(SysMonitorOshi.class);
  private final SystemInfo si = new SystemInfo();
  private final HardwareAbstractionLayer hal = si.getHardware();
  private final OperatingSystem os = si.getOperatingSystem();
  private final List<String> fsTypeWhitelist = ImmutableList.of("local");
  private final List<String> netAddressBlacklist = ImmutableList.of("0.0.0.0", "127.0.0.1");
  private final List<Stats> statsList;

  private final Map<String, String[]> dimensions;

  public SysMonitorOshi()
  {
    this(ImmutableMap.of());
  }

  public SysMonitorOshi(Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public SysMonitorOshi(Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);

    this.statsList = new ArrayList<Stats>();
    this.statsList.addAll(
        Arrays.asList(
            new MemStats(),
            new SwapStats(),
            new FsStats(),
            new DiskStats(),
            new NetStats(),
            new CpuStats(),
            new SysStats(),
            new TcpStats()
        )
    );
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Stats stats : statsList) {
      stats.emit(emitter);
    }
    return true;
  }

  private interface Stats
  {
    void emit(ServiceEmitter emitter);
  }

  private class MemStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      GlobalMemory mem = hal.getMemory();
      if (mem != null) {
        final Map<String, Long> stats = ImmutableMap.of(
            "sysO/mem/max", mem.getTotal(),
            "sysO/mem/used", mem.getTotal() - mem.getAvailable(),
            "sysO/mem/free", mem.getAvailable()
        );
        final ServiceMetricEvent.Builder builder = builder();
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class SwapStats implements Stats
  {
    private long prevPageIn = 0;
    private long prevPageOut = 0;

    @Override
    public void emit(ServiceEmitter emitter)
    {
      VirtualMemory swap = hal.getMemory().getVirtualMemory();

      if (swap != null) {
        long currPageIn = swap.getSwapPagesIn();
        long currPageOut = swap.getSwapPagesOut();
        final Map<String, Long> stats = ImmutableMap.of(
            "sysO/swap/pageIn", currPageIn - prevPageIn,
            "sysO/swap/pageOut", currPageOut - prevPageOut,
            "sysO/swap/max", swap.getSwapTotal(),
            "sysO/swap/free", swap.getSwapTotal() - swap.getSwapUsed()
        );

        final ServiceMetricEvent.Builder builder = builder();
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }

        this.prevPageIn = currPageIn;
        this.prevPageOut = currPageOut;
      }
    }
  }

  private class FsStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      FileSystem fileSystem = os.getFileSystem();
      for (OSFileStore fs : fileSystem.getFileStores()) {
        final String name = fs.getName();
        if (fsTypeWhitelist.contains(fs.getType())) {
          final Map<String, Long> stats = ImmutableMap.<String, Long>builder()
                                                      .put("sysO/fs/max", fs.getTotalSpace())
                                                      .put("sysO/fs/used", fs.getUsableSpace())
                                                      .put("sysO/fs/files/count", fs.getTotalInodes())
                                                      .put("sysO/fs/files/free", fs.getFreeInodes())
                                                      .build();
          final ServiceMetricEvent.Builder builder = builder()
              .setDimension("fsDevName", fs.getName())
              .setDimension("fsDirName", fs.getVolume())
              .setDimension("fsTypeName", fs.getType())
              .setDimension("fsSysTypeName", fs.getDescription())
              .setDimension("fsOptions", fs.getOptions().split(","));
          MonitorUtils.addDimensionsToBuilder(builder, dimensions);
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        } else {
          log.debug("Not monitoring fs stats for name[%s] with typeName[%s]", name, fs.getType());
        }
      }
    }
  }

  private class DiskStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      List<HWDiskStore> disks = hal.getDiskStores();
      // disk partitions can be mapped to file system but no inbuilt method is there to find relation b/w disks and file system
      // Will have to add logic for that
      for (HWDiskStore disk : disks) {
        final Map<String, Long> stats = ImmutableMap.<String, Long>builder()
                                                    .put("sysO/disk/read/size", disk.getReads())
                                                    .put("sysO/disk/read/count", disk.getReadBytes())
                                                    .put("sysO/disk/write/size", disk.getWrites())
                                                    .put("sysO/disk/write/count", disk.getWriteBytes())
                                                    .put("sysO/disk/queue", disk.getCurrentQueueLength())
                                                    .put("sysO/disk/transferTime", disk.getTransferTime())
                                                    .build();
        final ServiceMetricEvent.Builder builder = builder()
            .setDimension("diskName", disk.getName())
            .setDimension("diskModel", disk.getModel());
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class NetStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      List<NetworkIF> networkIFS = hal.getNetworkIFs();
      for (NetworkIF net : networkIFS) {
        final String name = net.getName();
        for (String addr : net.getIPv4addr()) {
          if (netAddressBlacklist.contains(addr)) {
            net = null;
            log.debug("Not monitoring network stats for ip addr: [%s], network name: [%s]", addr, name);
            break;
          }
        }
        if (net != null) {

          final Map<String, Long> stats = diff.to(
              name,
              ImmutableMap.<String, Long>builder()
                          .put("sysO/net/read/size", net.getBytesRecv())
                          .put("sysO/net/read/packets", net.getPacketsRecv())
                          .put("sysO/net/read/errors", net.getInErrors())
                          .put("sysO/net/read/dropped", net.getInDrops())
                          .put("sysO/net/write/size", net.getBytesSent())
                          .put("sysO/net/write/packets", net.getPacketsSent())
                          .put("sysO/net/write/errors", net.getOutErrors())
                          .put("sysO/net/write/collisions", net.getCollisions())
                          .build()
          );
          if (stats != null) {
            final ServiceMetricEvent.Builder builder = builder()
                .setDimension("netName", net.getName())
                .setDimension("netAddress", net.getMacaddr())
                .setDimension("netHwaddr", Arrays.toString(net.getIPv6addr()));
            MonitorUtils.addDimensionsToBuilder(builder, dimensions);
            for (Map.Entry<String, Long> entry : stats.entrySet()) {
              emitter.emit(builder.build(entry.getKey(), entry.getValue()));
            }
          }
        }
      }
    }
  }

  private class CpuStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();


    @Override
    public void emit(ServiceEmitter emitter)
    {
      CentralProcessor processor = hal.getProcessor();
      long[][] procTicks = processor.getProcessorCpuLoadTicks();
      for (int i = 0; i < procTicks.length; ++i) {
        final String name = Integer.toString(i);
        long[] ticks = procTicks[i];
        long user = ticks[TickType.USER.getIndex()];
        long nice = ticks[TickType.NICE.getIndex()];
        long sys = ticks[TickType.SYSTEM.getIndex()];
        long idle = ticks[TickType.IDLE.getIndex()];
        long iowait = ticks[TickType.IOWAIT.getIndex()];
        long irq = ticks[TickType.IRQ.getIndex()];
        long softirq = ticks[TickType.SOFTIRQ.getIndex()];
        long steal = ticks[TickType.STEAL.getIndex()];
        long totalCpu = user + nice + sys + idle + iowait + irq + softirq + steal;
        final Map<String, Long> stats = diff.to(
            name,
            ImmutableMap.<String, Long>builder()
                        .put("user", user) // user = Δuser / Δtotal
                        .put("sys", sys) // sys = Δsys / Δtotal
                        .put("nice", nice) // nice = Δnice / Δtotal
                        .put("wait", iowait) // wait = Δwait / Δtotal
                        .put("irq", irq) // irq = Δirq / Δtotal
                        .put("softIrq", softirq) // softIrq = ΔsoftIrq / Δtotal
                        .put("stolen", steal) // stolen = Δstolen / Δtotal
                        .put("_total", totalCpu) // (not reported)
                        .build()
        );
        if (stats != null) {
          final long total = stats.remove("_total");
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            final ServiceMetricEvent.Builder builder = builder()
                .setDimension("cpuName", name)
                .setDimension("cpuTime", entry.getKey());
            MonitorUtils.addDimensionsToBuilder(builder, dimensions);
            if (total != 0) {
              // prevent divide by 0 exception and don't emit such events
              emitter.emit(builder.build("sysO/cpu", entry.getValue() * 100 / total)); // [0,100]
            }

          }
        }
      }
    }
  }

  private class SysStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      long uptime = os.getSystemUptime();

      final Map<String, Number> stats = ImmutableMap.of(
          "sysO/uptime", uptime
      );
      for (Map.Entry<String, Number> entry : stats.entrySet()) {
        emitter.emit(builder.build(entry.getKey(), entry.getValue()));
      }
      CentralProcessor processor = hal.getProcessor();
      double[] la = processor.getSystemLoadAverage(3);

      if (la != null) {
        final Map<String, Number> statsCpuLoadAverage = ImmutableMap.of(
            "sysO/la/1", la[0],
            "sysO/la/5", la[1],
            "sysO/la/15", la[2]
        );
        for (Map.Entry<String, Number> entry : statsCpuLoadAverage.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class TcpStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      InternetProtocolStats ipstats = os.getInternetProtocolStats();
      InternetProtocolStats.TcpStats tcpv4 = ipstats.getTCPv4Stats();

      if (tcpv4 != null) {
        final Map<String, Long> stats = diff.to(
            "tcpv4", ImmutableMap.<String, Long>builder()
                                 .put("sysO/tcpv4/activeOpens", tcpv4.getConnectionsActive())
                                 .put("sysO/tcpv4/passiveOpens", tcpv4.getConnectionsPassive())
                                 .put("sysO/tcpv4/attemptFails", tcpv4.getConnectionFailures())
                                 .put("sysO/tcpv4/estabResets", tcpv4.getConnectionsReset())
                                 .put("sysO/tcpv4/in/segs", tcpv4.getSegmentsReceived())
                                 .put("sysO/tcpv4/in/errs", tcpv4.getInErrors())
                                 .put("sysO/tcpv4/out/segs", tcpv4.getSegmentsSent())
                                 .put("sysO/tcpv4/out/rsts", tcpv4.getOutResets())
                                 .put("sysO/tcpv4/retrans/segs", tcpv4.getSegmentsRetransmitted())
                                 .build()
        );
        if (stats != null) {
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        }
      }
      InternetProtocolStats.TcpStats tcpv6 = ipstats.getTCPv6Stats();

      if (tcpv6 != null) {
        final Map<String, Long> stats = diff.to(
            "tcpv6", ImmutableMap.<String, Long>builder()
                                 .put("sysO/tcpv6/activeOpens", tcpv6.getConnectionsActive())
                                 .put("sysO/tcpv6/passiveOpens", tcpv6.getConnectionsPassive())
                                 .put("sysO/tcpv6/attemptFails", tcpv6.getConnectionFailures())
                                 .put("sysO/tcpv6/estabResets", tcpv6.getConnectionsReset())
                                 .put("sysO/tcpv6/in/segs", tcpv6.getSegmentsReceived())
                                 .put("sysO/tcpv6/in/errs", tcpv6.getInErrors())
                                 .put("sysO/tcpv6/out/segs", tcpv6.getSegmentsSent())
                                 .put("sysO/tcpv6/out/rsts", tcpv6.getOutResets())
                                 .put("sysO/tcpv6/retrans/segs", tcpv6.getSegmentsRetransmitted())
                                 .build()
        );
        if (stats != null) {
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

}
