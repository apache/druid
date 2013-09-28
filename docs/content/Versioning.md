---
layout: doc_page
---
This page discusses how we do versioning and provides information on our stable releases.

Versioning Strategy
-------------------

We generally follow [semantic versioning](http://semver.org/). The general idea is

* "Major" version (leftmost): backwards incompatible, no guarantees exist about APIs between the versions
* "Minor" version (middle number): you can move forward from a smaller number to a larger number, but moving backwards *might* be incompatible.
* "bug-fix" version ("patch" or the rightmost): Interchangeable. The higher the number, the more things are fixed (hopefully), but the programming interfaces are completely compatible and you should be able to just drop in a new jar and have it work.

Note that this is defined in terms of programming API, **not** in terms of functionality. It is possible that a brand new awesome way of doing something is introduced in a "bug-fix" release version if it doesn’t add to the public API or change it.

One exception for right now, while we are still in major version 0, we are considering the APIs to be in beta and are conflating "major" and "minor" so a minor version increase could be backwards incompatible for as long as we are at major version 0. These will be communicated via email on the group.

For external deployments, we recommend running the stable release tag. Releases are considered stable after we have deployed them into our production environment and they have operated bug-free for some time.

Tagging strategy
----------------

Tags of the codebase are equivalent to release candidates. We tag the code every time we want to take it through our release process, which includes some QA cycles and deployments. So, it is not safe to assume that a tag is a stable release, it is a solidification of the code as it goes through our production QA cycle and deployment. Tags will never change, but we often go through a number of iterations of tags before actually getting a stable release onto production. So, it is recommended that if you are not aware of what is on a tag, to stick to the stable releases listed on the [Download](Download.html) page.
