---
id: contributing-to-docs
title: "How to contribute to Druid docs"
---

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

Druid is a [community-led project](https://druid.apache.org/community/) and we are delighted to receive contributions of anything from minor fixes to docs to big new features.

Druid docs contributors:

* Improve existing content
* Create new content

## Getting started

Druid docs contributors can open an issue about documentation, or contribute a change with a pull request (PR).

The open source Druid docs are located here:
https://druid.apache.org/docs/latest/design/index.html


If you need to update a Druid doc, locate and update the doc in the Druid repo, following the instructions below.

## Druid repo branches

The Druid team works on the `master` branch and then branches for a release, such as 25.0

See [CONTRIBUTING.md](https://github.com/apache/incubator-druid/blob/master/CONTRIBUTING.md) for instructions on contributing to Apache Druid.

## Before you begin

Before you can contribute to the Druid docs for the first time, you must complete the following steps:

  1. Fork the [Druid repo](https://github.com/apache/druid). Your fork will be the ```origin``` remote.
  2. Clone the Druid repo from your fork.
  3. Set up your remotes locally ```upstream``` in the Druid repo in ```.git/config```:
  ~~~~
  [remote "upstream"]
  	url = https://github.com/apache/druid.git
  	fetch = +refs/heads/*:refs/remotes/upstream/*
  	pushurl = no_push
  [branch "master"]
  	remote = upstream
  	merge = refs/heads/master
  [remote "origin"]
  		url = https://github.com/{my-git-id}/druid.git
  		fetch = +refs/heads/*:refs/remotes/upstream/*
  [branch "master"]
  		remote = origin
  		merge = refs/heads/master
  ~~~~

  For ```upstream```, ```push_url = no_push``` means you won’t accidentally push to upstream.
  Make sure to put your github id for {my-git-id}.
  4. ```git config --list --show-origin``` to make sure you’ve got your email configured. If you need to set your email, you can set it per repo or globally. Global instructions [here](https://docs.github.com/en/github-ae@latest/account-and-profile/setting-up-and-managing-your-github-user-account/managing-email-preferences/setting-your-commit-email-address#setting-your-commit-email-address-in-git).
  5. Install Docusaurus so that you can build the site locally. Run either `npm install` or `yarn install` in the `website` directory.

## Contributing

  1. On branch ```master```, fetch the latest commit:

  ~~~~
  git fetch upstream

  remote: Enumerating objects: 397, done.
  remote: Counting objects: 100% (341/341), done.
  remote: Compressing objects: 100% (181/181), done.
  remote: Total 397 (delta 118), reused 255 (delta 98), pack-reused 56
  Receiving objects: 100% (397/397), 266.19 KiB | 16.64 MiB/s, done.
  Resolving deltas: 100% (118/118), completed with 44 local objects.
  From https://github.com/apache/druid
     819d706082..6c9f926e3e  0.21.0       -> upstream/0.21.0
     8a3be6bccc..84aac4832d  master       -> upstream/master
   * [new tag]               druid-0.21.0 -> druid-0.21.0

  ➜ git reset --hard upstream/master
  HEAD is now at 84aac4832d Add feature to automatically remove rules based on retention period (#11164)
  ~~~~

  Now you're up to date.

  2. Create your working branch:
  ~~~~
  git checkout -b my-work
  ~~~~
  3. Make your changes, add, and commit:
  ~~~~
  git add my-change.md
  git commit -m "i made some changes"
  ~~~~
  4. Test changes locally by building the site and navigating to your changes. In `website`, run `docusaurus-start`. By default, this starts the site on `localhost:3000`. 
  5. Push your changes to your fork: ```origin```
  ~~~~
  git push --set-upstream origin my-work
  ~~~~
  6. Go to the Druid repo. GitHub should recognize that you have a new branch in your fork. Create a pull request from your Druid fork and branch to the `master` branch in the Apache Druid repo.

## Style guide

Before publishing new content or updating an existing topic, audit your documentation using this checklist to make sure your contributions align with existing documentation.

* Use descriptive link text. If a link downloads a file, make sure to indicate this action
* Use present tense where possible
* Avoid negative constructions when possible
* Use clear and direct language
* Use descriptive headings and titles
* Avoid using a present participle or gerund as the first word in a heading or title. A shortcut for this is to not start with a word that ends in `-ing`. For example, don't use "Configuring Druid." Use "Configure Druid."
* Use sentence case in document titles and headings
* Don’t use images of text or code samples
* Use SVG over PNG if available
* Provide an equivalent text explanation with each image
* Use the appropriate text-formatting. For example, make sure code snippets and property names are in code font and UI elements are bold. Additionally, you should generally avoid using bold or italics to emphasize certain words.
* Put conditional clauses before instructions
* Avoid gender-specific pronouns, instead use they
* Use second person — you instead of we
* When American spelling is different from Commonwealth/"British" spelling, use the American spelling
* Don’t use terms considered disrespectful. Refer to the Google’s [Word list](https://developers.google.com/style/word-list) for guidance and alternatives
* Use straight quotation marks and straight apostrophes
* Introduce a list, a table, or a procedure with an introductory sentence
