---
layout: doc_page
---

Recommendations
===============

# Some General guidelines

JVM Flags:

```
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
-Djava.io.tmpdir=<something other than /tmp which might be mounted to volatile tmpfs file system>
-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
-Dorg.jboss.logging.provider=slf4j
-Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.SLF4JLogger
-Dlog4j.shutdownCallbackRegistry=org.apache.druid.common.config.Log4jShutdown
-Dlog4j.shutdownHookEnabled=true
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+PrintGCTimeStamps
-XX:+PrintGCApplicationStoppedTime
-XX:+PrintGCApplicationConcurrentTime
-Xloggc:/var/logs/druid/historical.gc.log
-XX:+UseGCLogFileRotation
-XX:NumberOfGCLogFiles=50
-XX:GCLogFileSize=10m
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/logs/druid/historical.hprof
-XX:MaxDirectMemorySize=10240g
```

`ExitOnOutOfMemoryError` flag is only supported starting JDK 8u92 . For older versions, `-XX:OnOutOfMemoryError='kill -9 %p'` can be used.

`MaxDirectMemorySize` restricts jvm from allocating more than specified limit, by setting it to unlimited jvm restriction is lifted and OS level memory limits would still be effective. It's still important to make sure that Druid is not configured to allocate more off-heap memory than your machine has available. Important settings here include druid.processing.numThreads, druid.processing.numMergeBuffers, and druid.processing.buffer.sizeBytes.

Please note that above flags are general guidelines only. Be cautious and feel free to change them if necessary for the specific deployment.

Additionally, for large jvm heaps, here are a few Garbage Collection efficiency guidelines that have been known to help in some cases.
- Mount /tmp on tmpfs ( See http://www.evanjones.ca/jvm-mmap-pause.html )
- On Disk-IO intensive nodes (e.g. Historical and MiddleManager), GC and Druid logs should be written to a different disk than where data is written.
- Disable Transparent Huge Pages ( See https://blogs.oracle.com/linux/performance-issues-with-transparent-huge-pages-thp )
- Try disabling biased locking by using `-XX:-UseBiasedLocking` jvm flag. ( See https://dzone.com/articles/logging-stop-world-pauses-jvm )

# Use UTC Timezone

We recommend using UTC timezone for all your events and across on your nodes, not just for Druid, but for all data infrastructure. This can greatly mitigate potential query problems with inconsistent timezones. To query in a non-UTC timezone see [query granularities](../querying/granularities.html#period-granularities)

# SSDs

SSDs are highly recommended for historical and real-time nodes if you are not running a cluster that is entirely in memory. SSDs can greatly mitigate the time required to page data in and out of memory.

# JBOD vs RAID
Historical nodes store large number of segments on Disk and support specifying multiple paths for storing those. Typically, hosts have multiple disks configured with RAID which makes them look like a single disk to OS. RAID might have overheads specially if its not hardware controller based but software based. So, Historicals might get improved disk throughput with JBOD.

# Use Timeseries and TopN Queries Instead of GroupBy Where Possible

Timeseries and TopN queries are much more optimized and significantly faster than groupBy queries for their designed use cases. Issuing multiple topN or timeseries queries from your application can potentially be more efficient than a single groupBy query.

# Segment sizes matter

Segments should generally be between 300MB-700MB in size. Too many small segments results in inefficient CPU utilizations and 
too many large segments impacts query performance, most notably with TopN queries.

# Read FAQs

You should read common problems people have here:

1) [Ingestion-FAQ](../ingestion/faq.html)

2) [Performance-FAQ](../operations/performance-faq.html)
