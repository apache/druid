# How to Contribute

## GitHub Workflow

1. Fork the druid-io/druid repository into your GitHub account

https://github.com/druid-io/druid

2. Clone your fork of the GitHub repository

```sh
git clone git@github.com:<ghuser>/druid.git
```

Add a remote to keep up with upstream changes

```
git remote add upstream https://github.com/druid-io/druid.git
```

If you already have a copy, fetch upstream changes

```
git fetch upstream
```

3. Create a feature branch to work in

```
git checkout -b feature-xxx remotes/upstream/master
```

4. Work in your feature branch

```
git commit -a
```

5. Periodically rebase your changes

```
git pull --rebase
```

6. When done, "squash" your commits

```
git rebase -i upstream/master
```

Prefix commits using `s` (squash) or `f` (fixup) to merge extraneous commits.

7. Submit a pull-request

```
git push origin feature-xxx
```

Go to your Druid fork main page

https://github.com/<ghuser>/druid

If you recently pushed your changes GitHub will automatically pop up a
`Compare & pull request` button for any branches you recently pushed to. If you
click that button it will automatically offer you to submit your pull-request
to the druid-io/druid repository.

- Give your pull-request a meaningful title.
- In the description, explain your changes and the problem they are solving.

8. Addressing code review comments

Repeat steps 4. through 6. to address any code review comments and
rebase your changes if necessary.

Push your updated changes to update the pull request

```
git push origin [--force] feature-xxx
```

`--force` may be necessary to overwrite your existing pull request in case your
commit history was changed when performing the rebase.

Note: Be careful when using `--force` since you may lose data if you are not careful.

```
git push origin --force feature-xxx
```


# FAQ

### Help! I merged changes from upstream and cannot figure out how to resolve conflits when rebasing!

Never fear, if you occasionally merged upgstream/master, here is another way to squash your changes into a single commit

Rename your existing branch first

```
git branch -m feature-xxx-unclean
```

Checkout a new branch with the original name `feature-xxx` from upstream. This branch will supercede our old one.

```
git checkout -b feature-xxx upstream/master
```

Then merge your changes in your original feature branch `feature-xxx-unclean` and create a single commit.

```
git merge --squash feature-xxx-unclean
git commit
```

You can now submit this new branch and create or replace your existing pull request

```
git push origin [--force] feature-xxx:feature-xxx
```
