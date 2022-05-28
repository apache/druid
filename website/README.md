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

# Druid doc builder

This website was created with [Docusaurus](https://docusaurus.io/).

To view documentation run:

`npm install`

Then run:

`npm start`

The current version of the web site appears in your browser. Edit pages with
your favorite editor. Refresh the web page after each edit to review your changes.

## Dependencies

* [NodeJS](https://nodejs.org/en/download/). Use the version Docusaurus specifies, not a
newer one. (For example, if 12.x is requested, don't install 16.x.)
Docusaurus may require a version
newer than that available in your Linux package repository, but older than the
latest version. See
[this page](https://github.com/nodesource/distributions/blob/master/README.md) to
find the version required by Docusaurus.
* The [Yarn](https://classic.yarnpkg.com/en/) dependency from Docusaurus is optional.
(This Yarn is not the Hadoop resource manager, it is a package manager for Node.js).
* [Docusaurus](https://docusaurus.io/docs/installation). Installed automatically
as part of the the above `npm` commands.

## Variables

Documentation pages can refer to a number of special variables using the
`{{var}}` syntax:

* `DRUIDVERSION` - the version of Druid in which the page appears. Allows
creating links to files of the same version on GitHub.

The variables are not replaced when running the web site locally using the
`start` command above.

## Spellcheck

Please run a spellcheck before issuing a pull request to avoid a build failure
due to spelling issues. Run:

```bash
npm run link-lint
npm run spellcheck
```

If you introduce new (correctly spelled) project names or technical terms, add
them to the dictionary in the `.spelling` file in this directory. Also, terms
enclosed in backticks are not spell checked. Example: \``symbolName`\`
