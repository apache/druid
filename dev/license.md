<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

## License management

[All Apache projects are required to provide the `LICENSE` and `NOTICE` files](https://infra.apache.org/licensing-howto.html).
This document explains how we manage the license of Druid and the libraries that Druid uses.

When we do a new release, we distribute 2 packages, i.e., source code package and binary package. Since the binary package
includes not only Druid itself but also all the dependencies of Druid, its `LICENSE` and `NOTICE` files should cover those dependencies as well.
As a result, the contents of those files are different and managed separately for source code and binary packages.
Please read the sections below to understand how we manage the licenses and what to update when you want to add new dependencies.


### Licenses for source code packages

The licenses for source code packages are maintained _manually_ in [the `LICENSE` file](https://github.com/apache/druid/blob/master/LICENSE). 
The notices can be found in [the `NOTICE` file`](https://github.com/apache/druid/blob/master/NOTICE) similarly.


### Licenses for binary packages

The licenses for binary packages are maintained _automatically_.
All licenses (for both source code and binary releases) should be registered in [the `licenses.yaml` file](https://github.com/apache/druid/blob/master/licenses.yaml).
[`generate-binary-license.py`](https://github.com/apache/druid/blob/master/distribution/bin/generate-binary-license.py)
and [`generate-binary-notice.py`](https://github.com/apache/druid/blob/master/distribution/bin/generate-binary-notice.py)
will generate the `LICENSE` and `NOTICE` files automatically based on the registry.


### Adding licenses

#### When you adopt source codes from other projects into Druid

This requires you to update the license and notice for both source code and binary packages.
First, you should add a new entry in the [`LICENSE`]((https://github.com/apache/druid/blob/master/LICENSE)) for the dependency you are adding.
For example, [the `CalciteCnfHelper` class](https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/segment/filter/cnf/CalciteCnfHelper.java)
is adopted from Apache Calcite. This requires adding the following entries in the `LICENSE` file.

```
SOURCE/JAVA-CORE
    This product contains SQL query planning code adapted from Apache Calcite.
      * processing/src/main/java/org/apache/druid/segment/filter/cnf/CalciteCnfHelper.java
``` 

If the project provides its own notice, the contents of the notice should be included in the `NOTICE` file of Druid.

> **NOTE:** Most projects don't provide notice. If you are not sure whether you need to add a notice, then you probably
> don't have to unless the dependency you are adding is an Apache project.

Going back to the example of Apache Calcite, since all Apache projects provide the `NOTICE` file, you should add the
following in the [`NOTICE`](https://github.com/apache/druid/blob/master/NOTICE) file.

```
############ SOURCE/JAVA-CORE ############

================= Apache Calcite 1.10.0 =================
Apache Calcite
Copyright 2012-2016 The Apache Software Foundation

This product is based on source code originally developed
by DynamoBI Corporation, LucidEra Inc., SQLstream Inc. and others
under the auspices of the Eigenbase Foundation
and released as the LucidDB project.
```

Second, you also need to add a new entry in [the `licenses.yaml` file](https://github.com/apache/druid/blob/master/licenses.yaml)
as below.

```yaml
name: SQL query planning code adapted from Apache Calcite
license_category: source
module: java-core
license_name: Apache License version 2.0
source_paths:
  - processing/src/main/java/org/apache/druid/segment/filter/cnf/CalciteCnfHelper.java
```


#### When you add a new library dependency into Druid

This requires you to update the [the `licenses.yaml` file](https://github.com/apache/druid/blob/master/licenses.yaml).
For example, to add `aws-java-sdk-core` as a new depdency, you need to add the following entry.

```yaml
name: AWS SDK for Java
license_category: binary
module: java-core
license_name: Apache License version 2.0
version: 1.11.199
libraries:
  - com.amazonaws: aws-java-sdk-core
notice: |
  AWS SDK for Java
  Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  This product includes software developed by
  Amazon Technologies, Inc (http://www.amazon.com/).

  **********************
  THIRD PARTY COMPONENTS
  **********************
  This software includes third party software subject to the following copyrights:
  - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
  - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.

  The licenses for these third party components are included in LICENSE.txt
```

### Updating licenses

When you update the version of existing dependencies, you need to update the version not only in the `pom.xml` file but
also in the `licenses.yaml` file.