---
id: contribute-to-docs
title: "Contribute to Druid docs"
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

Apache Druid is a [community-led project](https://druid.apache.org/community/). We are delighted to receive contributions to the docs ranging from minor fixes to big new features.

Druid docs contributors:

* Improve existing content
* Create new content

## Getting started

Druid docs contributors can open an issue about documentation, or contribute a change with a pull request (PR).

The open source Druid docs are located here:
https://druid.apache.org/docs/latest/design/index.html


If you need to update a Druid doc, locate and update the doc in the Druid repo following the instructions below.

## Druid repo branches

The Druid team works on the `master` branch and then branches for a release, such as `26.0.0`.

See [`CONTRIBUTING.md`](https://github.com/apache/incubator-druid/blob/master/CONTRIBUTING.md) for instructions on contributing to Apache Druid.

## Before you begin

Before you can contribute to the Druid docs for the first time, you must complete the following steps:
1. Fork the [Druid repo](https://github.com/apache/druid). Your fork will be the `origin` remote.
2. Clone your fork: 
   
   ```bash
   git clone git@github.com:GITHUB_USERNAME/druid.git
   ```
   Replace `GITHUB_USERNAME` with your GitHub username.
3. In the directory where you cloned your fork, set up `apache/druid`  as your your remote `upstream` repo:
   
   ```bash
   git remote add upstream https://github.com/apache/druid.git
   ```
4. Confirm that your fork shows up as the origin repo and `apache/druid` shows up as the upstream repo:   

   ```bash
   git remote -v
   ```

5. Verify that you have your email configured for GitHub:

   ```bash
   git config user.email
   ```
   
   If you need to set your email, see the [GitHub instructions](https://docs.github.com/en/github-ae@latest/account-and-profile/setting-up-and-managing-your-github-user-account/managing-email-preferences/setting-your-commit-email-address#setting-your-commit-email-address-in-git).

5. Install Docusaurus so that you can build the site locally. Run either `npm install` or `yarn install` in the `website` directory.
   
## Contributing

Before you contribute, make sure your local branch of `master` and the upstream Apache branch are up-to-date and in sync. This can help you avoid merge conflicts. Run the following commands on your fork's `master` branch:

```bash
git fetch origin
git fetch upstream
```

Then run either one of the following commands:

```bash
git rebase upstream/master
# or
git merge upstream/master
```

Now you're up to date, and you can make your changes.

1. Create your working branch:

   ```bash
   git checkout -b MY-BRANCH
   ```

   Provide a name for your feature branch in `MY-BRANCH`.

2. Find the file that you want to make changes to. All the source files for the docs are written in Markdown and located in the `docs` directory. The URL for the page includes the subdirectory the source file is in. For example, the SQL-based ingestion tutorial found at `https://druid.apache.org/docs/latest/tutorials/tutorial-msq-extern.html` is in the `tutorials` subdirectory.
   
   If you're adding a page, create a new Markdown file in the appropriate subdirectory. Then, copy the front matter and Apache license from an existing file. Update the `title` and `id` fields. Don't forget to add it to `website/sidebars.json` so that your new page shows up in the navigation.

4. Test changes locally by building the site and navigating to your changes. In the `website` directory, run `docusaurus-start`. By default, this starts the site on `localhost:3000`. If port `3000` is already in use, it'll increment the port number from there.
   
5. Use the following commands to run the link and spellcheckers locally: 
   
   ```bash
   cd website
   # You only need to install once
   npm install
   npm run build

   npm run spellcheck
   npm run link-lint
   ```

   This step can save you time during the review process since they'll run faster than the GitHub Action version of the checks and warn you of issues before you create a PR.

5. Push your changes to your fork: 

   ```bash
   git push --set-upstream origin MY-BRANCH
   ```

6. Go to the Druid repo. GitHub should recognize that you have a new branch in your fork. Create a pull request from your Druid fork and branch to the `master` branch in the Apache Druid repo.

The pull request template is extensive. You may not need all the information there, so feel free to delete unneeded sections as you fill it out. Once you create the pull request, GitHub automatically labels the issue so that reviewers can take a look.

The docs go through a review process similar to the code where community members will offer feedback. Once the review process is complete and your changes are merged, they'll be available on the live site when the site gets republished. 

## Style guide

Consistent style, formatting, and tone make documentation easier to consume.
For the majority of style considerations, the Apache Druid documentation follows the [Google Developer Documentation Style Guide](https://developers.google.com/style).
The style guide should serve as a point of reference to enable contributors and reviewers to maintain documentation quality.

### Notable style exceptions

In some cases, Google Style might make the Druid docs more difficult to read and understand. This section highlights those exceptions.

#### SQL keyword syntax
For SQL keywords and functions, use all caps, but do not use code font.

:::tip

**Correct**

The UNNEST clause unnests array values.

**Incorrect**

The \`UNNEST\` clause unnests array values.
:::


#### Optional parameters and arguments

For optional parameters and arguments, enclose the optional parameter and leading command in brackets.

:::tip

**Correct**

HUMAN_READABLE_BINARY_BYTE_FORMAT(value[, precision])

**Incorrect**

HUMAN_READABLE_BINARY_BYTE_FORMAT(value, \[precision])
:::

#### Markdown table format

When editing or adding tables, do not include extra characters to "prettify" the table format within the Markdown source.
Some code editors may format tables by default.
See the developer [style guide](https://github.com/apache/druid/blob/master/dev/style-conventions.md) for more information.

:::tip

**Correct**

```markdown
| Column 1 | Column 2 | Column 3 |
| --- | --- | --- |
| value 1 | val 2 | a-very-long-value 3 |
```

**Incorrect**

```markdown
| Column 1 | Column 2 | Column 3            |
| -------- | -------- | ------------------- |
| value 1  | val 2    | a-very-long-value 3 |
```

:::

### Style checklist

Before publishing new content or updating an existing topic, you can audit your documentation using the following checklist to make sure your contributions align with existing documentation:

* Use descriptive link text. If a link downloads a file, make sure to indicate this action.
* Use present tense where possible.
* Avoid negative constructions when possible. In other words, try to tell people what they should do instead of what they shouldn't.
* Use clear and direct language.
* Use descriptive headings and titles.
* Avoid using a present participle or gerund as the first word in a heading or title. A shortcut for this is to not start with a word that ends in `-ing`. For example, don't use "Configuring Druid." Use "Configure Druid."
* Use sentence case in document titles and headings.
* Don’t use images of text or code samples.
* Use SVG over PNG for images if you can.
* Provide alt text or an equivalent text explanation with each image.
* Use the appropriate text-formatting. For example, make sure code snippets and property names are in code font and UI elements are bold. Generally, you should  avoid using bold or italics to emphasize certain words unless there's a good reason.
* Put conditional clauses before instructions. In the following example, "to drop a segment" is the conditional clause: to drop a segment, do the following.
* Avoid gender-specific pronouns, instead use "they."
* Use second person singular — "you" instead of "we."
* When American spelling is different from Commonwealth/"British" spelling, use the American spelling.
* Don’t use terms considered disrespectful. Refer to a list like Google’s [Word list](https://developers.google.com/style/word-list) for guidance and alternatives.
* Use straight quotation marks and straight apostrophes instead of the curly versions.
* Introduce a list, a table, or a procedure with an introductory sentence that prepares the reader for what they're about to read.
