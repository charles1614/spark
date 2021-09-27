#!/usr/bin/env bash

set -o pipefail
set -e
set -x

# Figure out where the Spark framework is installed
DISTDIR="$SPARK_HOME/dist"

NAME=blaze
VERSION="3.0.3-SNAPSHOT"
SPARK_HADOOP_VERSION=3.2.0

echo "Making spark-$VERSION-bin-$NAME.tgz"

# Build uber fat JAR
cd "$SPARK_HOME"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/jars"
echo "Spark $VERSION built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"

# Copy jars
cp "$SPARK_HOME"/assembly/target/scala*/jars/* "$DISTDIR/jars/"

# Only create the yarn directory if the yarn artifacts were built.
if [ -f "$SPARK_HOME"/common/network-yarn/target/scala*/spark-*-yarn-shuffle.jar ]; then
  mkdir "$DISTDIR/yarn"
  cp "$SPARK_HOME"/common/network-yarn/target/scala*/spark-*-yarn-shuffle.jar "$DISTDIR/yarn"
fi

# Only create and copy the dockerfiles directory if the kubernetes artifacts were built.
if [ -d "$SPARK_HOME"/resource-managers/kubernetes/core/target/ ]; then
  mkdir -p "$DISTDIR/kubernetes/"
  cp -a "$SPARK_HOME"/resource-managers/kubernetes/docker/src/main/dockerfiles "$DISTDIR/kubernetes/"
  cp -a "$SPARK_HOME"/resource-managers/kubernetes/integration-tests/tests "$DISTDIR/kubernetes/"
fi

# Copy examples and dependencies
mkdir -p "$DISTDIR/examples/jars"
cp "$SPARK_HOME"/examples/target/scala*/jars/* "$DISTDIR/examples/jars"

# Deduplicate jars that have already been packaged as part of the main Spark dependencies.
for f in "$DISTDIR"/examples/jars/*; do
  name=$(basename "$f")
  if [ -f "$DISTDIR/jars/$name" ]; then
    rm "$DISTDIR/examples/jars/$name"
  fi
done

# Copy example sources (needed for python and SQL)
mkdir -p "$DISTDIR/examples/src/main"
cp -r "$SPARK_HOME/examples/src/main" "$DISTDIR/examples/src/"

# Copy license and ASF files
if [ -e "$SPARK_HOME/LICENSE-binary" ]; then
  cp "$SPARK_HOME/LICENSE-binary" "$DISTDIR/LICENSE"
  cp -r "$SPARK_HOME/licenses-binary" "$DISTDIR/licenses"
  cp "$SPARK_HOME/NOTICE-binary" "$DISTDIR/NOTICE"
else
  echo "Skipping copying LICENSE files"
fi

if [ -e "$SPARK_HOME/CHANGES.txt" ]; then
  cp "$SPARK_HOME/CHANGES.txt" "$DISTDIR"
fi

# Copy data files
cp -r "$SPARK_HOME/data" "$DISTDIR"

# Copy other things
mkdir "$DISTDIR/conf"
cp "$SPARK_HOME"/conf/*.template "$DISTDIR/conf"
cp "$SPARK_HOME/README.md" "$DISTDIR"
cp -r "$SPARK_HOME/bin" "$DISTDIR"
cp -r "$SPARK_HOME/python" "$DISTDIR"


cp -r "$SPARK_HOME/sbin" "$DISTDIR"
# Copy SparkR if it exists
if [ -d "$SPARK_HOME/R/lib/SparkR" ]; then
  mkdir -p "$DISTDIR/R/lib"
  cp -r "$SPARK_HOME/R/lib/SparkR" "$DISTDIR/R/lib"
  cp "$SPARK_HOME/R/lib/sparkr.zip" "$DISTDIR/R/lib"
fi

TARDIR_NAME=spark-$VERSION-bin-$NAME
TARDIR="$SPARK_HOME/$TARDIR_NAME"
rm -rf "$TARDIR"
cp -r "$DISTDIR" "$TARDIR"
tar czf "spark-$VERSION-bin-$NAME.tgz" -C "$SPARK_HOME" "$TARDIR_NAME"
rm -rf "$TARDIR"
