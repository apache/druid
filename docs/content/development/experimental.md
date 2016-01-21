---
layout: doc_page
---
# About Experimental Features
Experimental features are features we have developed but have not fully tested in a production environment. If you choose to try them out, there will likely be edge cases that we have not covered. We would love feedback on any of these features, whether they are bug reports, suggestions for improvement, or letting us know they work as intended.


To enable experimental features, include their artifacts in the configuration runtime.properties file, e.g.,

```
druid.extensions.loadList=["druid-histogram"]
```

The configuration files for all the indexer and query nodes need to be updated with this.
