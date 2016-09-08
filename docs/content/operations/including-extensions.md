---
layout: doc_page
---

# Loading extensions

## Loading core extensions

Druid bundles all [core extensions](../development/extensions.html#core-extensions) out of the box. 
See the [list of extensions](../development/extensions.html#core-extensions) for your options. You 
can load bundled extensions by adding their names to your common.runtime.properties 
`druid.extensions.loadList` property. For example, to load the *postgresql-metadata-storage* and 
*druid-hdfs-storage* extensions, use the configuration:

```
druid.extensions.loadList=["postgresql-metadata-storage", "druid-hdfs-storage"]
```

These extensions are located in the `extensions` directory of the distribution.

<div class="note info">
Druid bundles two sets of configurations: one for the <a href="../tutorials/quickstart.html">quickstart</a> and 
one for a <a href="../tutorials/cluster.html">clustered configuration</a>. Make sure you are updating the correct 
common.runtime.properties for your setup.
</div>

<div class="note caution">
Because of licensing, the mysql-metadata-storage extension is not packaged with the default Druid tarball. In order to get it, you can download it from <a href="http://druid.io/downloads.html">druid.io</a>, 
then unpack and move it into the extensions directory. Make sure to include the name of the extension in the loadList configuration.
</div>

## Loading community and third-party extensions (contrib extensions)

You can also load community and third-party extensions not already bundled with Druid. To do this, first download the extension and 
then install it into your `extensions` directory. You can download extensions from their distributors directly, or 
if they are available from Maven, the included [pull-deps](../operations/pull-deps.html) can download them for you. To use *pull-deps*, 
specify the full Maven coordinate of the extension in the form `groupId:artifactId:version`. For example, 
for the (hypothetical) extension *com.example:druid-example-extension:1.0.0*, run: 

```
java \
  -cp "lib/*" \
  -Ddruid.extensions.directory="extensions" \
  -Ddruid.extensions.hadoopDependenciesDir="hadoop-dependencies" \
  io.druid.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "com.example:druid-example-extension:1.0.0"
```

You only have to install the extension once. Then, add `"druid-example-extension"` to 
`druid.extensions.loadList` in common.runtime.properties to instruct Druid to load the extension.

<div class="note info">
Please make sure all the Extensions related configuration properties listed <a href="../configuration/index.html">here</a> are set correctly.
</div>

<div class="note info">
The Maven groupId for almost every <a href="../development/extensions.html#community-extensions">community extension</a> is io.druid.extensions.contrib. The artifactId is the name 
of the extension, and the version is the latest Druid stable version.
</div>


## Loading extensions from classpath

If you add your extension jar to the classpath at runtime, Druid will also load it into the system.  This mechanism is relatively easy to reason about, 
but it also means that you have to ensure that all dependency jars on the classpath are compatible.  That is, Druid makes no provisions while using 
this method to maintain class loader isolation so you must make sure that the jars on your classpath are mutually compatible.
