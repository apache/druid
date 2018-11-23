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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.DirUsage;
import org.hyperic.sigar.DiskUsage;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.NetStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;
import org.hyperic.sigar.Tcp;
import org.hyperic.sigar.Uptime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SysMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(SysMonitor.class);

  private final Sigar sigar = SigarUtil.getSigar();

  private final List<String> fsTypeWhitelist = ImmutableList.of("local");
  private final List<String> netAddressBlacklist = ImmutableList.of("0.0.0.0", "127.0.0.1");

  private final List<Stats> statsList;

  private Map<String, String[]> dimensions;

  public SysMonitor()
  {
    this(ImmutableMap.of());
  }

  public SysMonitor(Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public SysMonitor(Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);

    sigar.enableLogging(true);

    this.statsList = new ArrayList<Stats>();
    this.statsList.addAll(
        Arrays.asList(
            new MemStats(),
            new FsStats(),
            new DiskStats(),
            new NetStats(),
            new CpuStats(),
            new SwapStats(),
            new SysStats(),
            new TcpStats()
        )
    );
  }

  public void addDirectoriesToMonitor(String[] dirList)
  {
    for (int i = 0; i < dirList.length; i++) {
      dirList[i] = dirList[i].trim();
    }
    statsList.add(new DirStats(dirList));
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
      Mem mem = null;
      try {
        mem = sigar.getMem();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Mem");
      }
      if (mem != null) {
        final Map<String, Long> stats = ImmutableMap.of(
            "sys/mem/max", mem.getTotal(),
            "sys/mem/used", mem.getUsed(),
            "sys/mem/actual/used", mem.getActualUsed(),
            "sys/mem/actual/free", mem.getActualFree()
        );
        final ServiceMetricEvent.Builder builder = builder();
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  /**
   * Gets the swap stats from sigar and emits the periodic pages in & pages out of memory
   * along with the max swap and free swap memory.
   */
  private class SwapStats implements Stats
  {
    private long prevPageIn = 0;
    private long prevPageOut = 0;

    private SwapStats()
    {
      try {
        Swap swap = sigar.getSwap();
        this.prevPageIn = swap.getPageIn();
        this.prevPageOut = swap.getPageOut();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Swap");
      }
    }

    @Override
    public void emit(ServiceEmitter emitter)
    {
      Swap swap = null;
      try {
        swap = sigar.getSwap();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Swap");
      }
      if (swap != null) {
        long currPageIn = swap.getPageIn();
        long currPageOut = swap.getPageOut();

        final Map<String, Long> stats = ImmutableMap.of(
            "sys/swap/pageIn", (currPageIn - prevPageIn),
            "sys/swap/pageOut", (currPageOut - prevPageOut),
            "sys/swap/max", swap.getTotal(),
            "sys/swap/free", swap.getFree()
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

  /**
   * Gets the disk usage of a particular directory.
   */
  private class DirStats implements Stats
  {
    private final String[] dirList;

    private DirStats(String[] dirList)
    {
      this.dirList = dirList;
    }

    @Override
    public void emit(ServiceEmitter emitter)
    {
      for (String dir : dirList) {
        DirUsage du = null;
        try {
          du = sigar.getDirUsage(dir);
        }
        catch (SigarException e) {
          log.error("Failed to get DiskUsage for [%s] due to   [%s]", dir, e.getMessage());
        }
        if (du != null) {
          final Map<String, Long> stats = ImmutableMap.of(
              "sys/storage/used", du.getDiskUsage()
          );
          final ServiceMetricEvent.Builder builder = builder()
              .setDimension("fsDirName", dir); // fsDirName because FsStats uses fsDirName
          MonitorUtils.addDimensionsToBuilder(builder, dimensions);
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  private class FsStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      FileSystem[] fss = null;
      try {
        fss = sigar.getFileSystemList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get FileSystem list");
      }
      if (fss != null) {
        log.debug("Found FileSystem list: [%s]", Joiner.on(", ").join(fss));
        for (FileSystem fs : fss) {
          final String name = fs.getDirName(); // (fs.getDevName() does something wonky here!)
          if (fsTypeWhitelist.contains(fs.getTypeName())) {
            FileSystemUsage fsu = null;
            try {
              fsu = sigar.getFileSystemUsage(name);
            }
            catch (SigarException e) {
              log.error(e, "Failed to get FileSystemUsage[%s]", name);
            }
            if (fsu != null) {
              final Map<String, Long> stats = ImmutableMap.<String, Long>builder()
                  .put("sys/fs/max", fsu.getTotal() * 1024)
                  .put("sys/fs/used", fsu.getUsed() * 1024)
                  .put("sys/fs/files/count", fsu.getFiles())
                  .put("sys/fs/files/free", fsu.getFreeFiles())
                  .build();
              final ServiceMetricEvent.Builder builder = builder()
                  .setDimension("fsDevName", fs.getDevName())
                  .setDimension("fsDirName", fs.getDirName())
                  .setDimension("fsTypeName", fs.getTypeName())
                  .setDimension("fsSysTypeName", fs.getSysTypeName())
                  .setDimension("fsOptions", fs.getOptions().split(","));
              MonitorUtils.addDimensionsToBuilder(builder, dimensions);
              for (Map.Entry<String, Long> entry : stats.entrySet()) {
                emitter.emit(builder.build(entry.getKey(), entry.getValue()));
              }
            }
          } else {
            log.debug("Not monitoring fs stats for name[%s] with typeName[%s]", name, fs.getTypeName());
          }
        }
      }
    }
  }

  private class DiskStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      FileSystem[] fss = null;
      try {
        fss = sigar.getFileSystemList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get FileSystem list");
      }
      if (fss != null) {
        log.debug("Found FileSystem list: [%s]", Joiner.on(", ").join(fss));
        for (FileSystem fs : fss) {
          // fs.getDevName() appears to give the same results here, but on some nodes results for one disc were substituted by another
          // LOG: Sigar - /proc/diskstats /dev/xvdj -> /dev/xvdb [202,16]
          final String name = fs.getDirName();
          if (fsTypeWhitelist.contains(fs.getTypeName())) {
            DiskUsage du = null;
            try {
              du = sigar.getDiskUsage(name);
            }
            catch (SigarException e) {
              log.error(e, "Failed to get DiskUsage[%s]", name);
            }
            if (du != null) {
              final Map<String, Long> stats = diff.to(
                  name,
                  ImmutableMap.<String, Long>builder()
                      .put("sys/disk/read/size", du.getReadBytes())
                      .put("sys/disk/read/count", du.getReads())
                      .put("sys/disk/write/size", du.getWriteBytes())
                      .put("sys/disk/write/count", du.getWrites())
                      .put("sys/disk/queue", Double.valueOf(du.getQueue()).longValue())
                      .put("sys/disk/serviceTime", Double.valueOf(du.getServiceTime()).longValue())
                      .build()
              );
              log.debug("DiskUsage diff for [%s]: %s", name, stats);
              if (stats != null) {
                final ServiceMetricEvent.Builder builder = builder()
                    .setDimension("fsDevName", fs.getDevName())
                    .setDimension("fsDirName", fs.getDirName())
                    .setDimension("fsTypeName", fs.getTypeName())
                    .setDimension("fsSysTypeName", fs.getSysTypeName())
                    .setDimension("fsOptions", fs.getOptions().split(","));
                MonitorUtils.addDimensionsToBuilder(builder, dimensions);
                for (Map.Entry<String, Long> entry : stats.entrySet()) {
                  emitter.emit(builder.build(entry.getKey(), entry.getValue()));
                }
              }
            }
          } else {
            log.debug("Not monitoring disk stats for name[%s] with typeName[%s]", name, fs.getTypeName());
          }
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
      String[] ifaces = null;
      try {
        ifaces = sigar.getNetInterfaceList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get NetInterface list");
      }
      if (ifaces != null) {
        log.debug("Found NetInterface list: [%s]", Joiner.on(", ").join(ifaces));
        for (String name : ifaces) {
          NetInterfaceConfig netconf = null;
          try {
            netconf = sigar.getNetInterfaceConfig(name);
          }
          catch (SigarException e) {
            log.error(e, "Failed to get NetInterfaceConfig[%s]", name);
          }
          if (netconf != null) {
            if (!(netAddressBlacklist.contains(netconf.getAddress()))) {
              NetInterfaceStat netstat = null;
              try {
                netstat = sigar.getNetInterfaceStat(name);
              }
              catch (SigarException e) {
                log.error(e, "Failed to get NetInterfaceStat[%s]", name);
              }
              if (netstat != null) {
                final Map<String, Long> stats = diff.to(
                    name,
                    ImmutableMap.<String, Long>builder()
                        .put("sys/net/read/size", netstat.getRxBytes())
                        .put("sys/net/read/packets", netstat.getRxPackets())
                        .put("sys/net/read/errors", netstat.getRxErrors())
                        .put("sys/net/read/dropped", netstat.getRxDropped())
                        .put("sys/net/read/overruns", netstat.getRxOverruns())
                        .put("sys/net/read/frame", netstat.getRxFrame())
                        .put("sys/net/write/size", netstat.getTxBytes())
                        .put("sys/net/write/packets", netstat.getTxPackets())
                        .put("sys/net/write/errors", netstat.getTxErrors())
                        .put("sys/net/write/dropped", netstat.getTxDropped())
                        .put("sys/net/write/collisions", netstat.getTxCollisions())
                        .put("sys/net/write/overruns", netstat.getTxOverruns())
                        .build()
                );
                if (stats != null) {
                  final ServiceMetricEvent.Builder builder = builder()
                      .setDimension("netName", netconf.getName())
                      .setDimension("netAddress", netconf.getAddress())
                      .setDimension("netHwaddr", netconf.getHwaddr());
                  MonitorUtils.addDimensionsToBuilder(builder, dimensions);
                  for (Map.Entry<String, Long> entry : stats.entrySet()) {
                    emitter.emit(builder.build(entry.getKey(), entry.getValue()));
                  }
                }
              }
            } else {
              log.debug("Not monitoring net stats for name[%s] with address[%s]", name, netconf.getAddress());
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
      Cpu[] cpus = null;
      try {
        cpus = sigar.getCpuList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Cpu list");
      }
      if (cpus != null) {
        log.debug("Found Cpu list: [%s]", Joiner.on(", ").join(cpus));
        for (int i = 0; i < cpus.length; ++i) {
          final Cpu cpu = cpus[i];
          final String name = Integer.toString(i);
          final Map<String, Long> stats = diff.to(
              name,
              ImmutableMap.<String, Long>builder()
                  .put("user", cpu.getUser()) // user = Δuser / Δtotal
                  .put("sys", cpu.getSys()) // sys = Δsys / Δtotal
                  .put("nice", cpu.getNice()) // nice = Δnice / Δtotal
                  .put("wait", cpu.getWait()) // wait = Δwait / Δtotal
                  .put("irq", cpu.getIrq()) // irq = Δirq / Δtotal
                  .put("softIrq", cpu.getSoftIrq()) // softIrq = ΔsoftIrq / Δtotal
                  .put("stolen", cpu.getStolen()) // stolen = Δstolen / Δtotal
                  .put("_total", cpu.getTotal()) // (not reported)
                  .build()
          );
          if (stats != null) {
            final long total = stats.remove("_total");
            for (Map.Entry<String, Long> entry : stats.entrySet()) {
              final ServiceMetricEvent.Builder builder = builder()
                  .setDimension("cpuName", name)
                  .setDimension("cpuTime", entry.getKey());
              MonitorUtils.addDimensionsToBuilder(builder, dimensions);
              emitter.emit(builder.build("sys/cpu", entry.getValue() * 100 / total)); // [0,100]
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

      Uptime uptime = null;
      try {
        uptime = sigar.getUptime();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Uptime");
      }

      double[] la = null;
      try {
        la = sigar.getLoadAverage();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Load Average");
      }

      if (uptime != null) {
        final Map<String, Number> stats = ImmutableMap.of(
            "sys/uptime", Double.valueOf(uptime.getUptime()).longValue()
        );
        for (Map.Entry<String, Number> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }

      if (la != null) {
        final Map<String, Number> stats = ImmutableMap.of(
            "sys/la/1", la[0],
            "sys/la/5", la[1],
            "sys/la/15", la[2]
        );
        for (Map.Entry<String, Number> entry : stats.entrySet()) {
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

      Tcp tcp = null;
      try {
        tcp = sigar.getTcp();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Tcp");
      }

      if (tcp != null) {
        final Map<String, Long> stats = diff.to(
            "tcp", ImmutableMap.<String, Long>builder()
                .put("sys/tcp/activeOpens", tcp.getActiveOpens())
                .put("sys/tcp/passiveOpens", tcp.getPassiveOpens())
                .put("sys/tcp/attemptFails", tcp.getAttemptFails())
                .put("sys/tcp/estabResets", tcp.getEstabResets())
                .put("sys/tcp/in/segs", tcp.getInSegs())
                .put("sys/tcp/in/errs", tcp.getInErrs())
                .put("sys/tcp/out/segs", tcp.getOutSegs())
                .put("sys/tcp/out/rsts", tcp.getOutRsts())
                .put("sys/tcp/retrans/segs", tcp.getRetransSegs())
                .build()
        );
        if (stats != null) {
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        }
      }

      NetStat netStat = null;
      try {
        netStat = sigar.getNetStat();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get NetStat");
      }
      if (netStat != null) {
        final Map<String, Long> stats = ImmutableMap.<String, Long>builder()
            .put("sys/net/inbound", (long) netStat.getAllInboundTotal())
            .put("sys/net/outbound", (long) netStat.getAllOutboundTotal())
            .put("sys/tcp/inbound", (long) netStat.getTcpInboundTotal())
            .put("sys/tcp/outbound", (long) netStat.getTcpOutboundTotal())
            .put(
                "sys/tcp/state/established",
                (long) netStat.getTcpEstablished()
            )
            .put("sys/tcp/state/synSent", (long) netStat.getTcpSynSent())
            .put("sys/tcp/state/synRecv", (long) netStat.getTcpSynRecv())
            .put("sys/tcp/state/finWait1", (long) netStat.getTcpFinWait1())
            .put("sys/tcp/state/finWait2", (long) netStat.getTcpFinWait2())
            .put("sys/tcp/state/timeWait", (long) netStat.getTcpTimeWait())
            .put("sys/tcp/state/close", (long) netStat.getTcpClose())
            .put("sys/tcp/state/closeWait", (long) netStat.getTcpCloseWait())
            .put("sys/tcp/state/lastAck", (long) netStat.getTcpLastAck())
            .put("sys/tcp/state/listen", (long) netStat.getTcpListen())
            .put("sys/tcp/state/closing", (long) netStat.getTcpClosing())
            .put("sys/tcp/state/idle", (long) netStat.getTcpIdle())
            .put("sys/tcp/state/bound", (long) netStat.getTcpBound())
            .build();
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }
}
