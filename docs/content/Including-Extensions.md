---
layout: doc_page
---
# Including Extensions

Druid uses a module system that allows for the addition of extensions at runtime.

## Specifying extensions

Druid extensions can be specified in the `common.runtime.properties`. There are two ways of adding druid extensions currently.

### Add to the classpath

If you add your extension jar to the classpath at runtime, Druid will load it into the system.  This mechanism is relatively easy to reason about, but it also means that you have to ensure that all dependency jars on the classpath are compatible.  That is, Druid makes no provisions while using this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.

### Specify maven coordinates

Druid has the ability to automatically load extension jars from maven at runtime.  With this mechanism, Druid also loads up the dependencies of the extension jar into an isolated class loader.  That means that your extension can depend on a different version of a library that Druid also uses and both can co-exist.

### I want classloader isolation, but I don't want my production machines downloading their own dependencies.  What should I do?

If you want to take advantage of the maven-based classloader isolation but you are also rightly frightened by the prospect of each of your production machines downloading their own dependencies on deploy, this section is for you.

The trick to doing this is

1) Specify a local directory for `druid.extensions.localRepository`

2) Run the `tools pull-deps` command to pull all the specified dependencies down into your local repository

3) Bundle up the local repository along with your other Druid stuff into whatever you use for a deployable artifact

4) Run Your druid processes with `druid.extensions.remoteRepositories=[]` and a local repository set to wherever your bundled "local" repository is located

The Druid processes will then only load up jars from the local repository and will not try to go out onto the internet to find the maven dependencies.
