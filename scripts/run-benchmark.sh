#!/usr/bin/env bash

java -cp ${PATH_TO_LDBS_SNB_IMPLEMENTATION:-../target/uber-janusgraph-1.0-SNAPSHOT.jar} org.ldbcouncil.snb.driver.Client -P ../validation/interactive-benchmark.properties
