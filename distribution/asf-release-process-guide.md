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

## Getting Started

### Announce intention to release

First up in performing an official release of Apache Druid (incubating) is to announce in the dev mailing list, dev@druid.apache.org, that it is about time for the next (approximately) quarterly release, or, that there is a critical bug that warrants doing a bug fix release, whatever the reason happens to be. Check for any critical bugs that are still open, or issues or PRs tagged with the release milestone, and give the community a bit of heads up to try and wrap up anything that _needs_ to be in the next release.

### Create a release branch

Next up is creating the release branch off of master (or the previous release branch for a bug fix release). Be sure to do this from the apache git repo, _not your fork_. Name the branch for the release version that is being created, omitting the `druid` prefix that will appear on the release candidate and release tags:

```bash
$ git checkout origin/0.15.0-incubating
...
$ git checkout -b 0.15.1-incubating
```

```bash
$ git checkout origin/master
...
$ git checkout -b 0.16.0-incubating
```

Then push the branch to `origin`. If doing a quarterly release, it will also be necessary to bump the version in master to the next release snapshot:

```bash
$ mvn versions:set -DnewVersion=0.17.0-incubating-SNAPSHOT
```

and open a PR to the master branch. Bug fix releases should already have the correct snapshot version from the previous releases on the branch, so this step is not necessary.

The only additions to the release branch after this branch should be bug fixes, which should be back-ported from the master branch, via a second PR, not with a direct PR to the release branch. Bug fix release branches may be initially populated via cherry-picking, but it is recommended to leave at least 1 commit to do as a backport PR in order to run through CI. (Note that CI is sometimes flaky for older branches).

Once all issues and PRs that are still tagged with the release milestone have been merged, closed, or removed from the milestone, the next step is to put together a release candidate.

## Initial setup to create a release candidate

### SVN access

Make sure that you have Apache SVN access set up, as several steps in the release process will require you to make SVN commits.

### GPG key

First, make sure you've set up a GPG key according to ASF instructions: https://www.apache.org/dev/openpgp.html

### Key propagation

The Apache guide suggests using the MIT keyserver, but if there are availability issues consider using https://sks-keyservers.net/ or http://pgp.surfnet.nl/

After your key has been propagated to public key servers, add your key fingerprint as your `OpenPGP Public Key Primary Fingerprint` at https://id.apache.org.

The key fingerprint can be seen with `gpg --list-keys`, e.g.:

```plaintext
...
pub   rsa4096 2019-02-28 [SC]
      0495E031C35D132479C241EC9283C4921AC4483E
uid           [ultimate] Jonathan Wei (CODE SIGNING KEY) <jonwei@apache.org>
sub   rsa4096 2019-02-28 [E]
...
```

`0495E031C35D132479C241EC9283C4921AC4483E` is the fingerprint in the example above.

If all goes well, your Apache account will be associated with that key, which you can check at https://people.apache.org/committer-index.html

You will also need to add your key to our project KEYS file: https://dist.apache.org/repos/dist/release/incubator/druid/KEYS, this repo can be pulled with:

```bash
$ svn co https://dist.apache.org/repos/dist/release/incubator/druid
```

Add your entry to the file and commit:

```bash
$ svn commit -m 'add key for jonwei@apache.org'
```

### Maven credentials

You'll need to configure Maven with your Apache credentials by adding the following to `~/.m2/settings.xml`. Encrypt the password you store here, see https://maven.apache.org/guides/mini/guide-encryption.html for details.

```xml
<settings>
 <servers>
   <!-- To publish a snapshot of some part of Maven -->
   <server>
     <id>apache.snapshots.https</id>
     <username>your-apache-username</username>
     <password>{your-encrypted-password}</password>
   </server>
   <!-- To stage a release of some part of Maven -->
   <server>
     <id>apache.releases.https</id>
     <username>your-apache-username</username>
     <password>{your-encrypted-password}</password>
   </server>
 </servers>
</settings>
```

## LICENSE and NOTICE handling

Before cutting a release candidate, the release manager should ensure that the contents of our `LICENSE` and `NOTICE` files are up-to-date.

The following links are helpful for understanding Apache's third-party licensing policies:

http://www.apache.org/dev/licensing-howto.html
http://www.apache.org/legal/src-headers.html
http://www.apache.org/legal/resolved.html


There are in effect 2 versions of the `LICENSE` and `NOTICE` file needed to perform a release, one set for the official ASF source release which are the actual `LICENSE` and `NOTICE` file in the root directory, and second for the convenience binary release which includes all of Druid's dependencies which we synthesize using some tools we have developed over the initial set of 'incubating' releases.

### licenses.yaml

At the core of these tools is a dependency registry file, `licenses.yaml`, that lives at the project root. `licenses.yaml` is a global registry of _all_ dependencies which are bundled with an official ASF source release or convenience binary release, divided into 'modules' in the following manner:

| 'module' | description |
| --- | --- |
| SOURCE/JAVA-CORE | Source level inclusions in the core Druid codebase |
| SOURCE/WEB-CONSOLE | Source level inclusions in the various web consoles |
| BINARY/JAVA-CORE | Bundled binary-only dependencies for core Druid components (i.e., not an extension, not a hadoop-dependency, not web console stuff) |
| BINARY/WEB-CONSOLE | Bundled binary-only dependencies for the various web consoles |
| BINARY/HADOOP-CLIENT | Bundled binary-only dependencies contained in the `hadoop-client` directory |
| BINARY/EXTENSIONS/{extension-name} | Bundled binary-only dependencies for Druid extensions |


`licenses.yaml` contains both `LICENSE` and (relevant) `NOTICE` file content which is used at distribution packaging time to build a `LICENSE.BINARY` and `NOTICE.BINARY` for the binary package.

If the dependency requires it, copy any licenses to the `licenses/src` or `licenses/bin` folder.

### LICENSE.BINARY and NOTICE.BINARY generator tools

| tool | description |
| --- | --- |
| [generate-binary-license](bin/generate-binary-license.py) | This script is run _automatically_ when building the distribution package to generate a `LICENSE.BINARY` file from `licenses.yaml` which is renamed to `LICENSE` in the binary package. |
| [generate-binary-notice](bin/generate-binary-notice.py) | This script is run _automatically_ when building the distribution package, and generates a `NOTICE.BINARY` file by appending the notice content of `licenses.yaml` to the source `NOTICE` file. This script does _not_ currently verify that all notices that need to be are present and correct, this must currently be done manually at release time if not done in the PR that changed a dependency. |
| [web-console/licenses](../web-console/script/licenses) | Updates `licenses.yaml` with all Druid of the licenses used by the Druid web-console 'binary'. |

### Additional tools

These additional tools were largely used to bootstrap the initial `LICENSE`, `LICENSE.BINARY`, `NOTICE`, and `NOTICE.BINARY` for our very first ASF releases and collect all the information we needed to eventually create `licenses.yaml`, and may still prove useful from time to time.

| tool | description |
| --- | --- |
| [generate-license-dependency-reports](bin/generate-license-dependency-reports.py) | Point this to the Druid source root, and give it the location of a temp scratch directory, and it will output Maven dependency reports for Druid. (I believe I had to generate Maven dep report separately for hadoop-client) |
| [check-licenses](bin/check-licenses.py) | Checks `licenses.yaml` against the output of `generate-license-dependency-reports.py`, used by travis and `-Papache-release` when building distribution, to verify that all dependencies are present and match the versions in `licenses.yaml`. |
| [jar-notice-lister](bin/jar-notice-lister.py) | Point this to an extracted Druid binary distribution, and give it a temp scratch directory, and it will output NOTICE information for all the Druid JAR files. |


The `licenses.yaml` dependency registry serves to help ease the process of managing releases and maintaining `LICENSE` and `NOTICE` compliance for a project as complex and with as many dependencies as Druid.

## Release notes

It is also the release managers responsibility for correctly assigning all PRs merged since the previous release with a milestone in github, as well as crafting the release notes. This is largely a manual process, but we have a couple of tools which can assist to some degree.

| tool | description |
| --- | --- |
| [get-milestone-contributors](bin/get-milestone-contributors.py) | lists github users who contributed to a milestone |
| [get-milestone-prs](bin/get-milestone-prs.py) | lists PRs between tags or commits and the milestone associated with them. |
| [tag-missing-milestones](bin/tag-missing-milestones.py) | Find pull requests which the milestone is missing and tag them properly. |
| [find-missing-backports](bin/find-missing-backports.py) | Find PRs which have been back-ported to one release branch but not another. Useful if a bug fix release based on the previous release is required during a release cycle. |


Next create an issue in the Druid github to contain the release notes and allow the community to provide feedback prior to the release. Make sure to attach it to the release milestone in github. It is highly recommended to review [previous release notes for reference](https://github.com/apache/incubator-druid/issues?utf8=%E2%9C%93&q=is%3Aissue+%22incubating+release+notes%22+label%3A%22Release+Notes%22+is%3Aclosed+) of how to best structure them. Be sure to call out any exciting new features, important bug fixes, and any compatibility concerns for users or operators to consider when upgrading to this release.


## Building a release candidate

### Set version and make a tag

Once the release branch is good for an RC, you can build a new tag with:

```bash
$ mvn -DreleaseVersion=0.16.0-incubating -DdevelopmentVersion=0.16.1-incubating-SNAPSHOT -Dtag=druid-0.16.0-incubating-rc3 -DpushChanges=false clean release:clean release:prepare
```

In this example it will create a tag, `druid-0.16.0-incubating-rc3`. If this release passes vote then we can add the final `druid-0.16.0-incubating` release tag later.

**Retain the release.properties file! You will need it when uploading the Maven artifacts for the final release.**

### Do a clean clone (so the source distribution does not pick up extra files)

```bash
$ git clone git@github.com:apache/incubator-druid.git druid-release
```

### Switch to tag

```bash
$ git checkout druid-0.16.0-incubating-rc3
```

### Build Artifacts

```bash
$ mvn clean install -Papache-release,dist,rat -DskipTests
```

This should produce the following artifacts:

```plaintext
apache-druid-0.16.0-incubating-bin.tar.gz
apache-druid-0.16.0-incubating-bin.tar.gz.asc
apache-druid-0.16.0-incubating-bin.tar.gz.sha512
apache-druid-0.16.0-incubating-src.tar.gz
apache-druid-0.16.0-incubating-src.tar.gz.asc
apache-druid-0.16.0-incubating-src.tar.gz.sha512
```

You _might_ or _might not_ need to specify which key to use depending on your keychain, supply `-Dgpg.keyname=<your GPG key fingerprint>` to the `mvn install` command if required.

### Verify checksums

```bash
$ diff <(shasum -a512 apache-druid-0.16.0-incubating-bin.tar.gz | cut -d ' ' -f1) <(cat apache-druid-0.16.0-incubating-bin.tar.gz.sha512 ; echo)
...
$ diff <(shasum -a512 apache-druid-0.16.0-incubating-src.tar.gz | cut -d ' ' -f1) <(cat apache-druid-0.16.0-incubating-src.tar.gz.sha512 ; echo)
```

### Verify GPG signatures

```bash
$ gpg --verify apache-druid-0.16.0-incubating-bin.tar.gz.asc apache-druid-0.16.0-incubating-bin.tar.gz
...
$ gpg --verify apache-druid-0.16.0-incubating-src.tar.gz.asc apache-druid-0.16.0-incubating-src.tar.gz
```

### Commit artifacts to SVN repo

Checkout the dist/dev repo and add a directory for the new release candidate. The KEYS file in this repo is obsoleted and should not be used.

```bash
$ svn checkout https://dist.apache.org/repos/dist/dev/incubator/druid
```

Copy the `src` and `bin` artifacts to a folder for the release version and add it to SVN and publish the artifacts:

```bash
$ svn add 0.16.0-incubating-rc3
...
$ svn commit -m 'add 0.16.0-incubating-rc3 artifacts'
```

### Update druid.staged.apache.org

1. Pull https://github.com/apache/incubator-druid-website and https://github.com/apache/incubator-druid-website-src. These repositories should be in the same directory as your Druid repository that should have the release tag checked out.

2. From incubator-druid-website, checkout branch `asf-staging`.

3. From incubator-druid-website-src, run `./release.sh 0.16.0-incubating 0.16.0-incubating`, replacing `0.16.0-incubating` where the first argument is the release version and 2nd argument is commit-ish. This script will:

* checkout the tag of the Druid release version
* build the docs for that version into incubator-druid-website-src
* build incubator-druid-website-src into incubator-druid-website
* stage incubator-druid-website-src and incubator-druid-website repositories to git.

4. Make a PR to the src repo (https://github.com/apache/incubator-druid-website-src) for the release branch. Once the website PR is pushed to `asf-site`, https://druid.staged.apache.org/ will be updated near immediately with the new docs.

## Release candidates and voting

Until we graduate from incubation, releases have two stages of voting:

- Druid community vote (dev@druid.apache.org, Druid PPMC votes are binding)
- Incubator vote (general@incubator.apache.org, Incubator PMC votes are binding)

### Druid PPMC Vote

For the Druid community vote, send an email to dev@druid.apache.org, using something like the following as a template, replacing values as necessary.

##### Subject

```plaintext
[VOTE] Release Apache Druid (incubating) 0.16.0 [RC3]
```

##### Body

```plaintext
Hi all,

I have created a build for Apache Druid (incubating) 0.16.0, release
candidate 3.

Thanks for everyone who has helped contribute to the release! You can read
the proposed release notes here:
https://github.com/apache/incubator-druid/issues/8369

The release candidate has been tagged in GitHub as
druid-0.16.0-incubating-rc3 (54d29e438a4df34d75e2385af6cefd1092c4ebb3),
available here:
https://github.com/apache/incubator-druid/releases/tag/druid-0.16.0-incubating-rc3

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/incubator/druid/0.16.0-incubating-rc3/

Staged druid.apache.org website documentation is available here:
https://druid.staged.apache.org/docs/0.16.0-incubating/design/index.html

A Docker image containing the binary of the release candidate can be
retrieved via:
docker pull apache/incubator-druid:0.16.0-incubating-rc3

artifact checksums
src:
1f25c55e83069cf7071a97c1e0d56732437dbac4ef373ed1ed72b5b618021b74c107269642226e80081354c8da2e92dc26f1541b01072a4720fd6cfe8dc161a8
bin:
0c4b71f077e28d2f4d3bc3f072543374570b98ec6a1918a5e1828e1da7e3871b5efb04070a8bcdbc172a817e43254640ce28a99757984be7d8dd3d607f1d870e
docker: df9b900d3726ce123a5c054768da1ea08eba6efe635ced5abc3ad72d6c835e2c

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/cwylie.asc

This key and the key of other committers can also be found in the project's
KEYS file here:
https://dist.apache.org/repos/dist/release/incubator/druid/KEYS

(If you are a committer, please feel free to add your own key to that file
by following the instructions in the file's header.)


Verify checksums:
diff <(shasum -a512 apache-druid-0.16.0-incubating-src.tar.gz | \
cut -d ' ' -f1) \
<(cat apache-druid-0.16.0-incubating-src.tar.gz.sha512 ; echo)

diff <(shasum -a512 apache-druid-0.16.0-incubating-bin.tar.gz | \
cut -d ' ' -f1) \
<(cat apache-druid-0.16.0-incubating-bin.tar.gz.sha512 ; echo)

Verify signatures:
gpg --verify apache-druid-0.16.0-incubating-src.tar.gz.asc \
apache-druid-0.16.0-incubating-src.tar.gz

gpg --verify apache-druid-0.16.0-incubating-bin.tar.gz.asc \
apache-druid-0.16.0-incubating-bin.tar.gz

Please review the proposed artifacts and vote. Note that Apache has
specific requirements that must be met before +1 binding votes can be cast
by PMC members. Please refer to the policy at
http://www.apache.org/legal/release-policy.html#policy for more details.

As part of the validation process, the release artifacts can be generated
from source by running:
mvn clean install -Papache-release,dist -Dgpg.skip

The RAT license check can be run from source by:
mvn apache-rat:check -Prat

This vote will be open for at least 72 hours. The vote will pass if a
majority of at least three +1 PMC votes are cast.

Once the vote has passed, the second stage vote will be called on the
Apache Incubator mailing list to get approval from the Incubator PMC.

[ ] +1 Release this package as Apache Druid (incubating) 0.16.0
[ ] 0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...

Thanks!

Apache Druid (incubating) is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is
required of all newly accepted projects until a further review indicates
that the infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF projects. While
incubation status is not necessarily a reflection of the completeness or
stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
```

Anyone can vote on a release. When voting, you should list out what exactly you checked and note whether or not your vote is binding. (It is binding if you are on the Druid PPMC, otherwise it is non-binding.) A sample vote would look like:

```plaintext
+1 (binding)

- Proper NOTICE, LICENSE, DISCLAIMER files are present in both src and bin packages.
- Verified .asc signatures and .sha512 checksums of both src and bin packages.
- Source tarball git.version file is present and correct.
- Source tarball builds and tests pass.
- Ran through quickstart on binary tarball.
```

If you find something wrong with a release that should block it, vote -1 and explain why. For example:

```plaintext
-1, because the src tarball appears to accidentally have binary jar files in it:

- foo.jar
- bar.jar
```

According to Apache release policy,

- Release votes should remain open for at least 72 hours.
- Release votes pass if a minimum of three positive votes, and more positive than negative votes, are cast.
- Releases may not be vetoed.
- Before casting +1 binding votes, individuals are required to download all signed source code packages onto their own hardware, verify that they meet all requirements of ASF policy, validate all cryptographic signatures, compile as provided, and test the result on their own platform.

### Druid PPMC vote result

Once the Druid community vote passes (or fails), close the vote with a thread like the following:

##### Subject

```plaintext
[RESULT][VOTE] Release Apache Druid (incubating) 0.16.0 [RC3]
```

##### Body

```plaintext
Thanks to everyone who participated in the vote! The results are as follows:

Clint Wylie: +1 (non-binding)
Jihoon Son: +1 (binding)
Furkan KAMACI: +1 (non-binding)
Julian Hyde: +1 (binding)
David Lim: +1 (binding)
Surekha Saharan: +1 (non-binding)

...
```

### IPMC vote

The next step is to raise a vote thread on the incubator PMC mailing list.

Before doing so, we create a staged maven repository containing the RC artifacts.

You can generate a staged maven repo for the RC with the following steps:

- Copy the `release.properties` that was generated when the RC tag was created back to your repo root (You'll need to change scm.tag in that file to match the specific rc tag you created if you did not specify `-Dtag` when creating the release). If you still have the working copy that build the `tag` available, you may use that.
- Run `mvn release:perform`

This will create a staged Maven repo here (login with Apache credentials): https://repository.apache.org/#stagingRepositories

To make the staged repo publicly accessible, "Close" the staging repo.

Here is the IPMC vote template. Please note that "6190EEFC" in the template is your key id which is the last 16 (or 8) hex digits your key.

##### Subject

```plaintext
[VOTE] Release Apache Druid (incubating) 0.16.0 [RC3]
```

##### Body

```plaintext
Hi IPMC,

The Apache Druid community has voted on and approved a proposal to release
Apache Druid (incubating) 0.16.0 (rc3).

We now kindly request the Incubator PMC members review and vote on this
incubator release.

Apache Druid (incubating) is a high performance analytics data store for
event-driven data.

The community voting thread can be found here:
https://lists.apache.org/thread.html/ea2f70b0533f403215288ae35294adc59f9ab33accabd9048eeb082f@%3Cdev.druid.apache.org%3E

We have already received 2 +1 votes from IPMC members (Julian Hyde and
Furkan KAMACI) during our community vote which I believe can be carried
over to this vote as +1 (binding).

During our previous IPMC release vote, a concern was raised about
licensing/distribution of a file containing example data, and we have
raised https://issues.apache.org/jira/browse/LEGAL-480 to get to the bottom
of this. The discussion is currently ongoing there, but at this point it
seems possible that including this file might end up being acceptable, so I
have decided to proceed with this vote now. If it turns out that we cannot
distribute this file then we intend to switch to a different example data
file in a future release.

The release notes are available here:
https://github.com/apache/incubator-druid/issues/8369

The release candidate has been tagged in GitHub as
druid-0.16.0-incubating-rc3 (54d29e438a4df34d75e2385af6cefd1092c4ebb3),
available here:
https://github.com/apache/incubator-druid/releases/tag/druid-0.16.0-incubating-rc3

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/incubator/druid/0.16.0-incubating-rc3/

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapachedruid-1009/

A Docker image containing the binary of the release candidate can be
retrieved via:
docker pull apache/incubator-druid:0.16.0-incubating-rc3

artifact checksums
src:
1f25c55e83069cf7071a97c1e0d56732437dbac4ef373ed1ed72b5b618021b74c107269642226e80081354c8da2e92dc26f1541b01072a4720fd6cfe8dc161a8
bin:
0c4b71f077e28d2f4d3bc3f072543374570b98ec6a1918a5e1828e1da7e3871b5efb04070a8bcdbc172a817e43254640ce28a99757984be7d8dd3d607f1d870e
docker: df9b900d3726ce123a5c054768da1ea08eba6efe635ced5abc3ad72d6c835e2c

Release artifacts are signed with the key (6190EEFC):
https://people.apache.org/keys/committer/cwylie.asc

This key and the key of other committers can be found in the project's KEYS
file here:
https://www.apache.org/dist/incubator/druid/KEYS

Staged druid.apache.org website documentation is available here:
https://druid.staged.apache.org/docs/0.16.0-incubating/design/index.html

As part of the validation process, the release artifacts can be generated
from source by running:
mvn clean install -Papache-release,dist

The RAT license check can be run from source by:
mvn apache-rat:check -Prat

This vote will be open for at least 72 hours. The vote will pass if a
majority of at least three +1 IPMC votes are cast.

[ ] +1 Release this package as Apache Druid (incubating) 0.16.0
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...

Thank you IPMC! We appreciate your efforts in helping the Apache Druid
community to validate this release.

On behalf of the Apache Druid Community,
Clint

Apache Druid (incubating) is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is
required of all newly accepted projects until a further review indicates
that the infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF projects. While
incubation status is not necessarily a reflection of the completeness or
stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
```

### Cancelling a vote

If for any reason during the Druid PPMC or IPMC vote a blocking issue becomes apparent, a vote should be officially cancelled by sending an email with the following subject line: `[CANCEL][VOTE] Release Apache Druid (incubating) 0.15.1 [RC1]` and the reasons for the cancellation in the body.

### Previous vote threads for additional examples

- [Druid PPMC](https://lists.apache.org/list.html?dev@druid.apache.org:lte=1y:%22%5BVOTE%5D%20Release%20Apache%20Druid%20(incubating)%22)
- [IPMC](https://lists.apache.org/list.html?general@incubator.apache.org:lte=1y:%22%5BVOTE%5D%20Release%20Apache%20Druid%20(incubating)%22)


## Final Release

Once a release candidate has passed the incubator PMC vote, you'll need to do the following:

- Close the IPMC vote thread
- Publish release artifacts to the release SVN repo
- Publish the staged Maven repo
- Wait 24 hours to allow the artifacts to propagate across mirror
- Update the druid.apache.org website
- Announce the release

### Close the IPMC vote thread

##### Subject

```plaintext
[RESULT] [VOTE] Release Apache Druid (incubating) 0.16.0 [RC3]
```

##### Body

```plaintext
Hi all,

The vote to release Apache Druid (incubating) 0.16.0 has passed with 3 +1
binding votes:

Julian Hyde
Furkan KAMACI
Gian Merlino


Vote thread:
https://lists.apache.org/thread.html/a2eff0f2ef609a8960265d1f32c27748c5ea69bd00f511eca2022e0f@%3Cgeneral.incubator.apache.org%3E


Thank you to the above IPMC members for taking the time to review and
provide guidance on our release!

We will proceed with publishing the approved artifacts and sending out the
appropriate announcements in the coming days.

On behalf of the Apache Druid Community,
Clint
```
### Create git tag

Tag the rc that passed vote as the release tag and push to github.

```bash
$ git checkout druid-0.16.0-incubating-rc3
$ git tag druid-0.16.0-incubating
$ git push origin/druid-0.16.0-incubating
```

### Publish release artifacts to SVN

The final release artifacts are kept in the following repo (same as KEYS):

```bash
$ svn checkout https://dist.apache.org/repos/dist/release/incubator/druid
```

Create a new directory for the release and put the artifacts there.

```bash
$ svn add 0.16.0-incubating
...
$ svn commit -m 'add 0.16.0-incubating artifacts'
```

### Publish the staged Maven repo
Returning to the staged repo you created for the IPMC vote ( https://repository.apache.org/#stagingRepositories), "Release" the repo to publish the Maven artifacts.

https://central.sonatype.org/pages/releasing-the-deployment.html#close-and-drop-or-release-your-staging-repository


### Wait 24 hours

Apache policy requires projects to wait at least 24 hours after uploading artifacts before announcing a release, to allow time for the release artifacts to propagate across mirrors.

http://www.apache.org/legal/release-policy.html#release-announcements


### Update druid.apache.org

1. Pull https://github.com/apache/incubator-druid-website and https://github.com/apache/incubator-druid-website-src. These repositories should be in the same directory as your Druid repository that should have the release tag checked out.

2. To update the downloads page of the website, update the _config.yml file in the root of the website src repo. Versions are grouped by release branch:

```yaml
druid_versions:
  - release: 0.16
    versions:
      - version: 0.16.0-incubating
        date: 2019-09-24
  - release: 0.15
    versions:
      - version: 0.15.1-incubating
        date: 2019-08-15
```

3. From incubator-druid-website-src, run `./release.sh 0.16.0-incubating 0.16.0-incubating`, replacing `0.16.0-incubating` where the first argument is the release version and 2nd argument is commit-ish. This script will:

* checkout the tag of the Druid release version
* build the docs for that version into incubator-druid-website-src
* build incubator-druid-website-src into incubator-druid-website
* stage incubator-druid-website-src and incubator-druid-website repositories to git.

4. Make a PR to the src repo (https://github.com/apache/incubator-druid-website-src) and to the website repo (https://github.com/apache/incubator-druid-website). Once the website PR is merged, https://druid.apache.org/ will be updated immediately.

### Draft a release on github

Copy the release notes and create the release from the tag.


### Announce the release

Announce the release to all the lists, announce@apache.org, general@incubator.apache.org, dev@druid.apache.org, druid-user@googlegroups.com (general announcement list, incubator general list, druid dev list, druid user group).

Additionally, announce it to the Druid official ASF Slack channel, https://druid.apache.org/community/join-slack.

**Be sure to include the Apache incubation disclaimer.**

##### subject

```plaintext
[ANNOUNCE] Apache Druid (incubating) 0.16.0 release
```

##### body

```plaintext
The Apache Druid team is proud to announce the release of Apache Druid
(incubating) 0.16.0. Druid is a high performance analytics data store for
event-driven data.

Apache Druid 0.16.0-incubating contains over 350 new features, performance
enhancements, bug fixes, and major documentation improvements from 50
contributors. Major new features and improvements include:

- 'Vectorized' query processing
- 'Minor' compaction
- Native parallel indexing with shuffle
- New 'indexer' process
- Huge improvements to the web console
- New documentation website
- Official Docker image

Source and binary distributions can be downloaded from:
https://druid.apache.org/downloads.html

Release notes are at:
https://github.com/apache/incubator-druid/releases/tag/druid-0.16.0-incubating

A big thank you to all the contributors in this milestone release!

----

Disclaimer: Apache Druid is an effort undergoing incubation at The Apache
Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is
required of all newly accepted projects until a further review indicates
that the infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF projects. While
incubation status is not necessarily a reflection of the completeness or
stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
```

[Previous release announcements](https://lists.apache.org/list.html?general@incubator.apache.org:lte=1y:%22%5BANNOUNCE%5D%20Apache%20Druid%20(incubating%22))

Sticky the announcement on the ['druid-user' group](https://groups.google.com/forum/?pli=1#!forum/druid-user).

### Update Wikipedia

Update the release version and date on the [Apache Druid Wikipedia article](https://en.wikipedia.org/w/index.php?title=Apache_Druid) (bots might also do this?).

### Remove old releases which are not 'active'

Removing old releases from the ['release' SVN repository](https://dist.apache.org/repos/dist/release/incubator/druid) which are not actively being developed is an Apache policy. The contents of this directory controls which artifacts are available on the Apache download mirror network. All other releases are available from the Apache archives, so removal from here is not a permanent deletion.

We consider the 'active' releases to be the latest versions of the current and previous quarterly releases. If you are adding a new quarterly release, remove the oldest quarterly release. If you are adding a bug fix release, remove the current quarterly release. 

For example, if the new release is a bug fix, `0.15.1-incubating`, and the current versions are `0.15.0-incubating`, and `0.14.2-incubating`, then you would delete `0.15.0-incubating`, leaving `0.14.2-incubating` and `0.15.1-incubating` on the mirrors. If instead we were adding `0.16.0-incubating`, the resulting set of 'active' releases would be `0.15.0-incubating` and `0.16.0-incubating`.
