#!/bin/bash -e

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

opt_api=1
opt_docs=1
while getopts ":adn" opt; do
  case $opt in
    n)
      opt_dryrun="1"
      ;;
    d)
      opt_api=
      ;;
    a)
      opt_docs=
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done
shift $((OPTIND-1))

# Set $version to Druid version (tag will be "druid-$version")
if [ -z "$1" ]; then 
    version="latest"
else
    version=$1
fi

# Set $origin to name of origin remote
if [ -z "$2" ]; then
    origin="origin"
else
    origin=$2
fi

# Use s3cmd if available, otherwise try awscli
if command -v s3cmd >/dev/null 2>&1
then
  s3sync="s3cmd sync --delete-removed"
else
  s3sync="aws s3 sync --delete"
fi

# Location of git repository containing this script
druid=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

if [ -n "$(git -C "$druid" status --porcelain --untracked-files=no)" ]; then
  echo "Working directory is not clean, aborting"
  exit 1
fi

branch=druid-$version
if [ "$version" == "latest" ]; then
  branch=master
fi

if [ -z "$(git tag -l "$branch")" ] && [ "$branch" != "master" ]; then
  echo "Version tag does not exist: druid-$version"
  exit 1;
fi

tmp=$(mktemp -d -t druid-docs-deploy)
target=$tmp/docs
src=$tmp/druid

echo "Using Version     [$version]"
echo "Working directory [$tmp]"

git clone -q --depth 1 git@github.com:apache/incubator-druid-io.github.io.git "$target"

remote=$(git -C "$druid" config --local --get "remote.$origin.url")
git clone -q --depth 1 --branch $branch $remote "$src"

if [ -n "$opt_docs" ] ; then
  # Check for broken links
  "$src/docs/_bin/broken-link-check.py" "$src/docs/content" "$src/docs/_redirects.json"

  # Copy docs
  mkdir -p $target/docs/$version
  rsync -a --delete "$src/docs/content/" $target/docs/$version

  # Replace #{DRUIDVERSION} with current Druid version
  # Escaping of $version is weak here, but it should be fine for typical version strings
  find "$target/docs/$version" -name "*.md" -print0 | xargs -0 perl -pi -e's/\#\{DRUIDVERSION\}/'"$version"'/g'

  # Create redirects
  "$src/docs/_bin/make-redirects.py" "$target/docs/$version" "$src/docs/_redirects.json"
fi

# generate javadocs for releases (not for master)
if [ "$version" != "latest" ] && [ -n "$opt_api" ] ; then
  (cd $src && mvn javadoc:aggregate)
  mkdir -p $target/api/$version
  if [ -z "$opt_dryrun" ]; then
    $s3sync "$src/target/site/apidocs/" "s3://static.druid.io/api/$version/"
  fi
fi

updatebranch=update-docs-$version

git -C $target checkout -b $updatebranch
git -C $target add -A .
git -C $target commit -m "Update $version docs"
if [ -z "$opt_dryrun" ]; then
  git -C $target push origin $updatebranch

  if [ -n "$GIT_TOKEN" ]; then
  curl -u "$GIT_TOKEN:x-oauth-basic" -XPOST -d@- \
     https://api.github.com/repos/apache/incubator-druid-io.github.io/pulls <<EOF
{
  "title" : "Update Documentation for $version",
  "head"  : "$updatebranch",
  "base"  : "src"
}
EOF

  else
    echo "GitHub personal token not provided, not submitting pull request"
    echo "Please go to https://github.com/apache/incubator-druid-io.github.io and submit a pull request from the \`$updatebranch\` branch"
  fi

  rm -rf $tmp
else
  echo "Not pushing. To see changes run:"
  echo "git -C $target diff HEAD^1"
fi
