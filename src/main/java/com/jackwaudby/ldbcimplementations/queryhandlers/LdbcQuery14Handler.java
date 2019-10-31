package com.jackwaudby.ldbcimplementations.queryhandlers;

/**
 * Given two Persons, find all (unweighted) shortest paths between these two Persons,
 * in the sub- graph induced by the Knows relationship.
 * Then, for each path calculate a weight.
 * The nodes in the path are Persons, and the weight of a path is the sum of weights between
 * every pair of consecutive Person nodes in the path.
 * The weight for a pair of Persons is calculated such that every reply (by one of the Persons) to
 * a Post (by the other Person) contributes 1.0, and every reply (by ones of the Persons) to
 * a Comment (by the other Person) contributes 0.5.
 * Return all the paths with shortest length, and their weights.
 * Do not return any rows if there is no path between the two Persons.
 */
public class LdbcQuery14Handler {
    // get shortest paths
    // calculate weight with sack
}
