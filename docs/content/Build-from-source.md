---
layout: doc_page
---
### Clone and Build from Source

The other way to setup Druid is from source via git. To do so, run these commands:

```
git clone git@github.com:metamx/druid.git
cd druid
./build.sh
```

You should see a bunch of files: 

```
DruidCorporateCLA.pdf	README			common			examples		indexer			pom.xml			server
DruidIndividualCLA.pdf	build.sh		doc			group_by.body		install			publications		services
LICENSE			client			eclipse_formatting.xml	index-common		merger			realtime
```

You can find the example executables in the examples/bin directory:

* run_example_server.sh
* run_example_client.sh
