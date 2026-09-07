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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * SysMonitor implemented using {@link oshi}
 * <p>
 * Following stats are emitted:
 * <ul>
 *   <li>{@link MemStats} for Memory related metrics</li>
 *   <li>{@link SwapStats} for swap storage related metrics</li>
 *   <li>{@link FsStats} for File System related Metrics</li>
 *   <li>{@link DiskStats} for Disk level metrics</li>
 *   <li>{@link NetStats} for Network Interface and related metrics</li>
 *   <li>{@link CpuStats} for CPU usage and stats metrics</li>
 *   <li>{@link SysStats} for overall system metrics(uptime, avg load)</li>
 *   <li>{@link TcpStats} for TCP related metrics</li>
 * </ul>
 */
public class OshiSysMonitor extends FeedDefiningMonitor
{

  private final SystemInfo si;
  private final HardwareAbstractionLayer hal;
  private final OperatingSystem os;
  private static final List<String> NET_ADDRESS_BLACKLIST = ImmutableList.of("0.0.0.0", "127.0.0.1");
  private final MemStats memStats;
  private final SwapStats swapStats;
  private final FsStats fsStats;
  private final DiskStats diskStats;
  private final NetStats netStats;
  private final CpuStats cpuStats;
  private final SysStats sysStats;
  private final TcpStats tcpStats;

  private final OshiSysMonitorConfig config;
  private final Map<String, Consumer<ServiceEmitter>> monitoringFunctions = ImmutableMap.of(
      "mem", this::monitorMemStats,
      "swap", this::monitorSwapStats,
      "fs", this::monitorFsStats,
      "disk", this::monitorDiskStats,
      "net", this::monitorNetStats,
      "cpu", this::monitorCpuStats,
      "sys", this::monitorSysStats,
      "tcp", this::monitorTcpStats
  );

  public OshiSysMonitor(OshiSysMonitorConfig config)
  {
    this(config, new SystemInfo());
  }

  // Create an object with mocked systemInfo for testing purposes
  @VisibleForTesting
  public OshiSysMonitor(OshiSysMonitorConfig config, SystemInfo systemInfo)
  {
    super(DEFAULT_METRICS_FEED);
    this.config = config;

    this.si = systemInfo;
    this.hal = si.getHardware();
    this.os = si.getOperatingSystem();

    this.memStats = new MemStats();
    this.swapStats = new SwapStats();
    this.fsStats = new FsStats();
    this.diskStats = new DiskStats();
    this.netStats = new NetStats();
    this.cpuStats = new CpuStats();
    this.sysStats = new SysStats();
    this.tcpStats = new TcpStats();
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    monitoringFunctions.forEach((key, function) -> {
      if (config.shouldEmitMetricCategory(key)) {
        function.accept(emitter);
      }
    });
    return true;
  }

  // Emit stats for a particular stat(mem, swap, filestore, etc) from statsList for testing
  public void monitorMemStats(ServiceEmitter emitter)
  {
    memStats.emit(emitter);
  }

  public void monitorSwapStats(ServiceEmitter emitter)
  {
    swapStats.emit(emitter);
  }

  public void monitorFsStats(ServiceEmitter emitter)
  {
    fsStats.emit(emitter);
  }

  public void monitorDiskStats(ServiceEmitter emitter)
  {
    diskStats.emit(emitter);
  }

  public void monitorNetStats(ServiceEmitter emitter)
  {
    netStats.emit(emitter);
  }

  public void monitorCpuStats(ServiceEmitter emitter)
  {
    cpuStats.emit(emitter);
  }

  public void monitorSysStats(ServiceEmitter emitter)
  {
    sysStats.emit(emitter);
  }

  public void monitorTcpStats(ServiceEmitter emitter)
  {
    tcpStats.emit(emitter);
  }

  /**
   * Implementation of Memstats
   * <p>
   * Define a method {@link #emit(ServiceEmitter)} to emit metrices in emiters
   */

  private class MemStats
  {
    public void emit(ServiceEmitter emitter)
    {
      GlobalMemory mem = hal.getMemory();
      if (mem != null) {
        final Map<String, Long> stats = ImmutableMap.of(
            "sys/mem/max",
            mem.getTotal(),
            "sys/mem/used",
            mem.getTotal() - mem.getAvailable(),
            // This is total actual memory used, not including cache and buffer memory
            "sys/mem/free",
            mem.getAvailable()
        );
        final ServiceMetricEvent.Builder builder = builder();
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class SwapStats
  {
    private long prevPageIn = 0;
    private long prevPageOut = 0;

    public void emit(ServiceEmitter emitter)
    {
      VirtualMemory swap = hal.getMemory().getVirtualMemory();

      if (swap != null) {
        long currPageIn = swap.getSwapPagesIn();
        long currPageOut = swap.getSwapPagesOut();
        final Map<String, Long> stats = ImmutableMap.of(
            "sys/swap/pageIn", currPageIn - prevPageIn,
            "sys/swap/pageOut", currPageOut - prevPageOut,
            "sys/swap/max", swap.getSwapTotal(),
            "sys/swap/free", swap.getSwapTotal() - swap.getSwapUsed()
        );

        final ServiceMetricEvent.Builder builder = builder();
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
        }

        this.prevPageIn = currPageIn;
        this.prevPageOut = currPageOut;
      }
    }
  }

  private class FsStats
  {
    public void emit(ServiceEmitter emitter)
    {
      FileSystem fileSystem = os.getFileSystem();
      for (OSFileStore fs : fileSystem.getFileStores(true)) { // get only local file store : true

        final Map<String, Long> stats = ImmutableMap.<String, Long>builder()
                                                    .put("sys/fs/max", fs.getTotalSpace())
                                                    .put("sys/fs/used", fs.getTotalSpace() - fs.getUsableSpace())
                                                    .put("sys/fs/files/count", fs.getTotalInodes())
                                                    .put("sys/fs/files/free", fs.getFreeInodes())
                                                    .build();
        final ServiceMetricEvent.Builder builder = builder()
            .setDimension("fsDevName", fs.getVolume())
            .setDimension("fsDirName", fs.getMount());
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class DiskStats
  {
    // Difference b/w metrics of two consecutive values. It tells Δmetric (increase/decrease in metrics value)
    private final KeyedDiff diff = new KeyedDiff();

    public void emit(ServiceEmitter emitter)
    {
      List<HWDiskStore> disks = hal.getDiskStores();
      // disk partitions can be mapped to file system but no inbuilt method is there to find relation b/w disks and file system
      // Will have to add logic for that
      for (HWDiskStore disk : disks) {

        final Map<String, Long> stats = diff.to(
            disk.getName(),
            ImmutableMap.<String, Long>builder()
                        .put("sys/disk/read/size", disk.getReadBytes())
                        .put("sys/disk/read/count", disk.getReads())
                        .put("sys/disk/write/size", disk.getWriteBytes())
                        .put("sys/disk/write/count", disk.getWrites())
                        .put("sys/disk/queue", disk.getCurrentQueueLength())
                        .put("sys/disk/transferTime", disk.getTransferTime())
                        .build()
        );
        if (stats != null) {
          final ServiceMetricEvent.Builder builder = builder()
              .setDimension("diskName", disk.getName());
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  private class NetStats
  {
    private final KeyedDiff diff = new KeyedDiff();

    public void emit(ServiceEmitter emitter)
    {
      List<NetworkIF> networkIFS = hal.getNetworkIFs();
      for (NetworkIF net : networkIFS) {
        final String name = net.getName();
        for (String addr : net.getIPv4addr()) {
          if (!NET_ADDRESS_BLACKLIST.contains(addr)) {
            // Only emit metrics for non black-listed ip addresses
            String mapKey = name
                            + "_"
                            + addr;    // Network_Name_IPV4 address as key, ex: wifi_192.1.0.1 to uniquely identify the dimension
            final Map<String, Long> stats = diff.to(
                mapKey,
                ImmutableMap.<String, Long>builder()
                            .put("sys/net/read/size", net.getBytesRecv())
                            .put("sys/net/read/packets", net.getPacketsRecv())
                            .put("sys/net/read/errors", net.getInErrors())
                            .put("sys/net/read/dropped", net.getInDrops())
                            .put("sys/net/write/size", net.getBytesSent())
                            .put("sys/net/write/packets", net.getPacketsSent())
                            .put("sys/net/write/errors", net.getOutErrors())
                            .put("sys/net/write/collisions", net.getCollisions())
                            .build()
            );
            if (stats != null) {
              final ServiceMetricEvent.Builder builder = builder()
                  .setDimension("netName", net.getName())
                  .setDimension("netAddress", addr)
                  .setDimension("netHwaddr", net.getMacaddr());
              for (Map.Entry<String, Long> entry : stats.entrySet()) {
                emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
              }
            }
          }
        }
      }
    }
  }

  private class CpuStats
  {
    private final KeyedDiff diff = new KeyedDiff();


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
                        .put("idle", idle) // idle = Δidle / Δtotal
                        .put("_total", totalCpu) // (not reported)
                        .build()
        );
        if (stats != null) {
          final long total = stats.remove("_total");
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            final ServiceMetricEvent.Builder builder = builder()
                .setDimension("cpuName", name)
                .setDimension("cpuTime", entry.getKey());
            if (total != 0) {
              // prevent divide by 0 exception and don't emit such events
              emitter.emit(builder.setMetric("sys/cpu", entry.getValue() * 100 / total)); // [0,100]
            }

          }
        }
      }
    }
  }

  private class SysStats
  {

    public void emit(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = builder();
      long uptime = os.getSystemUptime();

      final Map<String, Number> stats = ImmutableMap.of(
          "sys/uptime", uptime
      );
      for (Map.Entry<String, Number> entry : stats.entrySet()) {
        emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
      }
      CentralProcessor processor = hal.getProcessor();
      double[] la = processor.getSystemLoadAverage(3);

      if (la != null) {
        final Map<String, Number> statsCpuLoadAverage = ImmutableMap.of(
            "sys/la/1", la[0],
            "sys/la/5", la[1],
            "sys/la/15", la[2]
        );
        for (Map.Entry<String, Number> entry : statsCpuLoadAverage.entrySet()) {
          emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  private class TcpStats
  {
    private final KeyedDiff diff = new KeyedDiff();

    public void emit(ServiceEmitter emitter)
    {
      final ServiceMetricEvent.Builder builder = builder();

      InternetProtocolStats ipstats = os.getInternetProtocolStats();
      InternetProtocolStats.TcpStats tcpv4 = ipstats.getTCPv4Stats();

      if (tcpv4 != null) {
        final Map<String, Long> stats = diff.to(
            "tcpv4", ImmutableMap.<String, Long>builder()
                                 .put("sys/tcpv4/activeOpens", tcpv4.getConnectionsActive())
                                 .put("sys/tcpv4/passiveOpens", tcpv4.getConnectionsPassive())
                                 .put("sys/tcpv4/attemptFails", tcpv4.getConnectionFailures())
                                 .put("sys/tcpv4/estabResets", tcpv4.getConnectionsReset())
                                 .put("sys/tcpv4/in/segs", tcpv4.getSegmentsReceived())
                                 .put("sys/tcpv4/in/errs", tcpv4.getInErrors())
                                 .put("sys/tcpv4/out/segs", tcpv4.getSegmentsSent())
                                 .put("sys/tcpv4/out/rsts", tcpv4.getOutResets())
                                 .put("sys/tcpv4/retrans/segs", tcpv4.getSegmentsRetransmitted())
                                 .build()
        );
        if (stats != null) {
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

}
