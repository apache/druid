---
layout: doc_page
---

Evaluate Druid
==============

This page is meant to help you in evaluating Druid by answering common questions that come up.

## Evaluating on a Single Machine

Most of the tutorials focus on running multiple Druid services on a single machine in an attempt to teach basic Druid concepts, and work out kinks in data ingestion. The configurations in the tutorials are
 very poor choices for an actual production cluster.

## Capacity and Cost Planning

The best way to understand what your cluster will cost is to first understand how much data reduction you will get when you create segments.
We recommend indexing and creating segments from 1G of your data and evaluating the resultant segment size. This will allow you to see how much your data rolls up, and how many segments will be able
to be loaded on the hardware you have at your disposal.

Most of the cost of a Druid cluster is in historical nodes, followed by real-time indexing nodes if you have a high data intake. For high availability, you should have backup
coordination nodes (coordinators and overlords). Coordination nodes should require much cheaper hardware than nodes that serve queries.

## Selecting Hardware

Druid is designed to run on commodity hardware and we've tried to provide some general guidelines on [how things should be tuned]() for various deployments. We've also provided
some [example specs](../configuration/production-cluster.html) for hardware for a production cluster.

## Benchmarking Druid

The best resource to benchmark Druid is to follow the steps outlined in our [blog post](http://druid.io/blog/2014/03/17/benchmarking-druid.html) about the topic.
The code to reproduce the results in the blog post are all open source. The blog post covers Druid queries on TPC-H data, but you should be able to customize
 configuration parameters to your data set. The blog post is a little outdated and uses an older version of Druid, but is still mostly relevant to demonstrate performance.
