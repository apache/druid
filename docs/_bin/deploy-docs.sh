#! /bin/bash -e

if [ -z "$1" ]; then 
    version="latest"
else
    version=$1
fi

docs=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/docs

if [ -n "$(git -C "$docs" status --porcelain --untracked-files=no content)" ]; then
  echo "Docs directory is not clean, aborting"
  exit 1
fi

if [ -z "$(git tag -l "druid-$version")" ] && [ "$version" != "latest" ]; then
  echo "Version tag does not exist: druid-$version"
  exit 1;
fi

tmp=$(mktemp -d -t druid-docs-deploy)

echo "Using Version     [$version]"
echo "Working directory [$tmp]"

git clone git@github.com:druid-io/druid-io.github.io.git "$tmp"

target=$tmp/docs/$version

mkdir -p $target
rsync -a --delete "$docs/content/" $target

branch=update-docs-$version

git -C $tmp checkout  -b $branch
git -C $tmp add -A .
git -C $tmp commit -m "Update $version docs"
git -C $tmp push origin $branch

if [ -n "$GIT_TOKEN" ]; then
curl -u "$GIT_TOKEN:x-oauth-basic" -XPOST -d@- \
     https://api.github.com/repos/druid-io/druid-io.github.io/pulls <<EOF
{
  "title" : "Update $version docs",
  "head"  : "$branch",
  "base"  : "master"
}
EOF

else
  echo "GitHub personal token not provided, not submitting pull request"
  echo "Please go to https://github.com/druid-io/druid-io.github.io and submit a pull request from the \`$branch\` branch"
fi

rm -rf $tmp
