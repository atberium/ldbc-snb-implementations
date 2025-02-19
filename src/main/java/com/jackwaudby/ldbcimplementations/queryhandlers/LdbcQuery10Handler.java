package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery10Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.util.stream.Collectors.toList;

/**
 * Given a start Person (rootPerson), find that Person’s friends of friends (person).
 * Exclude rootPerson, and immediate friends, who were born on or after the 21st of a given month (in any year) and
 * before the 22nd of the following month.
 * Calculate the similarity between each person and rootPerson, where commonInterestScore is defined as follows:
 * - common = number of Posts created by person, such that the Post has a Tag that rootPerson is interested in
 * - uncommon = number of Posts created by person, such that the Post has no Tag that rootPerson is interested in
 * - commonInterestScore = common - uncommon
 * <p>
 * Return personId, personFirstName, personLastName, personGender, city person is located in (personCityName) and
 * commonInterestScore. Sorted by commonInterestScore (DESC) and personId (ASC). Limit 10
 */
public class LdbcQuery10Handler extends GremlinHandler implements OperationHandler<LdbcQuery10, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery10 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final long personId = operation.getPersonIdQ10();
        final long month = operation.getMonth();
        final long nextMonth = month != 12 ? month + 1 : 1;
        final long limit = operation.getLimit();


        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").sideEffect(out('hasInterest').store('tags'))." +
                "repeat(both('knows').simplePath()).times(2).dedup()." +
                "where(values('birthday').map{it.get().getMonth()}.is(eq(" + month + "-1))." +
                "and().values('birthday').map{it.get().getDate()}.is(gte(21))." +
                "or().values('birthday').map{it.get().getMonth()}.is(eq(" + nextMonth + "-1))." +
                "and().values('birthday').map{it.get().getDate()}.is(lt(22)))." +
                "local(union(__.in('hasCreator').hasLabel('Post').where(out('hasTag').dedup()." +
                "where(within('tags')).count().is(gt(0))).count()," +
                "__.in('hasCreator').hasLabel('Post').count()," +
                "values('id','firstName','lastName','gender')," +
                "out('isLocatedIn').values('name')).fold())." +
                "map{it -> [" +
                "commonInterestScore:[it.get().getAt(0) - (it.get().getAt(1)-it.get().getAt(0))]," +
                "id:[it.get().getAt(2)], " +
                "firstName:[it.get().getAt(3)]," +
                "lastName:[it.get().getAt(4)], " +
                "gender:[it.get().getAt(5)], " +
                "city:[it.get().getAt(6)],]}." +
                "order().by(select('commonInterestScore').unfold(),desc).by(select('id').unfold(),asc).limit(" + limit + ")";

        final List<LdbcQuery10Result> endResult = request(client, queryString).stream()
                .map(GremlinResponseParsers::resultToMap)
                .map(r -> new LdbcQuery10Result(
                        parseId(r),
                        parseFirstName(r),
                        parseLastName(r),
                        parseIntValue(r, "commonInterestScore"),
                        parsePersonGender(r),
                        parseStringValue(r, "city")
                ))
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
