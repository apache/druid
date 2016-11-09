---
layout: doc_page
---
# ResetCluster tool

ResetCluster tool can be used to completely wipe out Druid cluster state stored on Metadata and Deep storage. This is
intended to be used in dev/test environments where you typically want to reset the cluster before running
the test suite.
ResetCluster automatically figures out necessary information from Druid cluster configuration. So the java classpath
used in the command must have all the necessary druid configuration files.

It can be run in one of the following ways.

```
java io.druid.cli.Main tools reset-cluster [--metadataStore] [--segmentFiles] [--taskLogs] [--hadoopWorkingPath]
```

or

```
java io.druid.cli.Main tools reset-cluster --all
```

# Further Description
Usage documentation can be printed by running following command.

```
java io.druid.cli.Main help tools reset-cluster
```

```
NAME
        druid tools reset-cluster - Cleanup all persisted state from metadata
        and deep storage.

SYNOPSIS
        druid tools reset-cluster [--all] [--hadoopWorkingPath]
                [--metadataStore] [--segmentFiles] [--taskLogs]

OPTIONS
        --all
            delete all state stored in metadata and deep storage

        --hadoopWorkingPath
            delete hadoopWorkingPath

        --metadataStore
            delete all records in metadata storage

        --segmentFiles
            delete all segment files from deep storage

        --taskLogs
            delete all tasklogs
```
