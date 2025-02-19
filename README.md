# LDBC SNB Implementation for JanusGraph

This implementation assumes JanusGraph running as embedded Java application.
All paths mentioned in configuration parameters and environments bellow should be provided in the same environment as Janusgraph server runs (Docker container, virtual machine, etc).

## Dealing with dependencies

```bash
export CURRENT_DIR=$(pwd)
git clone git@github.com:ldbc/ldbc_snb_interactive_v1_driver.git /path/to/ldbc_snb_interactive_v1_driver
cd /path/to/ldbc_snb_interactive_v1_driver
mvn clean install -DskipTests
cd $CURRENT_DIR
mvn install:install-file \
   -Dfile=/path/to/ldbc_snb_interactive_v1_driver/target/driver-1.3.0-SNAPSHOT.jar \
   -DgroupId=org.ldbcouncil.snb \
   -DartifactId=driver \
   -Dversion=1.3.0-SNAPSHOT \
   -Dpackaging=jar \
   -DgeneratePom=true
```

## Set environments

```bash
# path to application jar
export PATH_TO_LDBS_SNB_IMPLEMENTATION=./target/uber-janusgraph-1.0-SNAPSHOT.jar
```

## Step-by-step Guide ##

Put all scripts from the `scripts` directory onto the same environment were Janusgraph server runs (Docker container, virtual machine, etc).

```bash
# load schema, indexes, vertices and edges
./complete-loader.sh
# set driver configuration for validation in interactive-validate.properties
# run validation
./run-validation.sh
```

## JanusGraph Console ##

Used for testing:

```
bin/gremlin.sh
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje-test.properties')
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')

g = graph.traversal()
```

## Validation Set ##

| Validation Set |              |
|----------------|--------------|
| Data Format:   | CSVComposite |
| Operations:    | 1321         |
| SF:            | 0.3          |
| Vertex Count:  | ~210k        |
| Edge Count:    | ~1.09m       |
| Load Time:     | ~2:30mins    |
| Index Time:    | ~2:45mins    |

Passing Validation:

+ Short Reads 7/7
+ Complex Reads 12/14
+ Updates 8/8

Missing handler implementations for 0 operation type(s)

| Operation     | Incorrect Result |
|---------------|------------------|
| `LdbcQuery1`  | 1                |
| `LdbcQuery14` | 1                |

Issues:

+ Validation's expected answer for `LdbcQuery1` is including the start person which it should not (see Complex Read 1
  in [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf)). This could imply a problem with
  the Cypher query used to generate the validation set.
+ My `LdbcQuery12` is not ordering by `personId`.
+ Implementation of `LdbcQuery14` does not handle the case when there are multiple shortest paths.
+ Problem with `LdbcShortQuery7MessageReplies` not returning results.
