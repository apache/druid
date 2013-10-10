---
layout: doc_page
---

Druid is an open-source analytics data store designed for real-time, exploratory, queries on large-scale data sets (100’s of Billions entries, 100’s TB data). Druid provides for cost effective, always-on, realtime data ingestion and arbitrary data exploration.

-   Try out Druid with our Getting Started [Tutorial](./Tutorial%3A-A-First-Look-at-Druid.html)
-   Learn more by reading the [White Paper](http://static.druid.io/docs/druid.pdf)

Why Druid?
----------

Druid was originally created to resolve query latency issues seen with trying to use Hadoop to power an interactive service. Hadoop has shown the world that it’s possible to house your data warehouse on commodity hardware for a fraction of the price of typical solutions. As people adopt Hadoop for their data warehousing needs, they find two things.

1.  They can now query all of their data in a fairly flexible manner and answer any question they have
2.  The queries take a long time

The first one is the joy that everyone feels the first time they get Hadoop running. The latter is what they realize after they have used Hadoop interactively for a while because Hadoop is optimized for throughput, not latency. Druid is a system that you can set up in your organization next to Hadoop. It provides the ability to access your data in an interactive slice-and-dice fashion. It trades off some query flexibility and takes over the storage format in order to provide the speed.

Druid is especially useful if you are summarizing your data sets and then querying the summarizations. If you put your summarizations into Druid, you will get quick queryability out of a system that you can be confident will scale up as your data volumes increase. Deployments have scaled up to 2TB of data per hour at peak ingested and aggregated in real-time.

We have more details about the general design of the system and why you might want to use it in our [White Paper](http://static.druid.io/docs/druid.pdf) or in our [Design](Design.html) doc.

The data store world is vast, confusing and constantly in flux. This page is meant to help potential evaluators decide whether Druid is a good fit for the problem one needs to solve. If anything about it is incorrect please provide that feedback on the mailing list or via some other means, we will fix this page.

#### When Druid?
* You need to do interactive, fast, exploration of large amounts of data
* You need analytics (not key value store)
* You have a lot of data (10s of Billions of events added per day, 10s of TB of data added per day)
* You want to do your analysis on data as it’s happening (realtime)
* Your store needs to be always-on, 24x7x365 and years into the future.

#### Not Druid?
* The amount of data you have can easily be handled by MySql
* Your querying for individual entries or doing lookups (Not Analytics)
* Batch is good enough
* Canned queries is good enough
* Downtime is no big deal

#### Druid vs…
* [Druid-vs-Impala-or-Shark](Druid-vs-Impala-or-Shark.html)
* [Druid-vs-Redshift](Druid-vs-Redshift.html)
* [Druid-vs-Vertica](Druid-vs-Vertica.html)
* [Druid-vs-Cassandra](Druid-vs-Cassandra.html)
* [Druid-vs-Hadoop](Druid-vs-Hadoop.html)

Key Features
------------

-   **Designed for Analytics** - Druid is built for exploratory analytics for OLAP workflows (streamalytics). It supports a variety of filters, aggregators and query types and provides a framework for plugging in new functionality. Users have leveraged Druid’s infrastructure to develop features such as top K queries and histograms.
-   **Interactive Queries** - Druid’s low latency data ingestion architecture allows events to be queried milliseconds after they are created. Druid’s query latency is optimized by only reading and scanning exactly what is needed. Aggregate and filter on data without sitting around waiting for results.
-   **Highly Available** - Druid is used to back SaaS implementations that need to be up all the time. Your data is still available and queryable during system updates. Scale up or down without data loss.
-   **Scalable** - Existing Druid deployments handle billions of events and terabytes of data per day. Druid is designed to be petabyte scale.
