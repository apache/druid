---
layout: doc_page
---

# Booting a Druid Cluster
[Loading Your Data](../tutorials/tutorial-loading-batch-data.html) and [All About Queries](../tutorials/tutorial-all-about-queries.html) contain recipes to boot a small druid cluster on localhost. However, when it's time to run a more realistic setup&mdash;for production or just for testing production&mdash;you'll want to find a way to start the cluster on multiple hosts. This document describes two different ways to do this: manually, or as a cloud service via Apache Whirr.

## Manually Booting a Druid Cluster
You can provision individual servers, loading Druid onto each machine (or building it) and setting the required configuration for each type of node. You'll also have to set up required external dependencies. Then you'll have to start each node. This process is outlined in [Tutorial: The Druid Cluster](../tutorials/tutorial-the-druid-cluster.html).

