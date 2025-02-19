package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.NonNull;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery3Result;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

/**
 * Title: Friends and friends of friends that have been to countries X and Y
 * <p>
 * Description: Given a start Person, find Persons that are their friends and friends of friends
 * that have made Posts/Comments in both of the given Countries, X and Y, within a given period.
 * Only Persons that are foreign to Countries X and Y are considered, that is Persons whose
 * Location is not Country X or Country Y.
 * <p>
 * Return: PersonID, firstName, lastName, xCount, yCount, count. Sort by xCount (desc) and personID (asc)
 */
public class LdbcQuery3Handler extends GremlinHandler implements OperationHandler<LdbcQuery3, JanusGraphDb.JanusGraphConnectionState> {

    private static long parseCountryXCount(@NonNull List<?> resultBreakdown, @NonNull String countryX) {
        return ofNullable(parseStringValue(toMap(resultBreakdown.get(3)), countryX))
                .map(Long::parseLong)
                .orElseGet(() -> parseLong(requireNonNull(parseStringValue(toMap(resultBreakdown.get(4)), countryX))));
    }

    private static long parseCountryYCount(@NonNull List<?> resultBreakdown, @NonNull String countryY) {
        return ofNullable(parseStringValue(toMap(resultBreakdown.get(4)), countryY))
                .map(Long::parseLong)
                .orElseGet(() -> parseLong(requireNonNull(parseStringValue(toMap(resultBreakdown.get(3)), countryY))));
    }

    @Override
    public void executeOperation(LdbcQuery3 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long personId = operation.getPersonIdQ3();
        final String countryX = operation.getCountryXName();
        final String countryY = operation.getCountryYName();
        final long startDate = operation.getStartDate().getTime();
        final long duration = operation.getDurationDays();
        final long endDate = startDate + (duration * 86400000L);
        final long limit = 20;

        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        final String queryString = "g.V().has('Person','id'," + personId + ").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "where(and(out('isLocatedIn').out('isPartOf').has('name',without('" + countryX + "','" + countryY + "'))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryX + "').count().is(gt(0)))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryY + "').count().is(gt(0)))))." +
                "order().by(__.in('hasCreator').has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name','" + countryX + "').count(),desc).by('id',asc).limit(" + limit + ")." +
                "local(union(identity().valueMap('id','firstName','lastName').unfold(),__.in('hasCreator')." +
                "has('creationDate',between(new Date(" + startDate + "),new Date(" + endDate + ")))." +
                "out('isLocatedIn').has('name',within('" + countryX + "','" + countryY + "')).group().by('name').by(count().fold()).unfold()).fold())";

        final List<LdbcQuery3Result> endResult = request(client, queryString).stream()
                .map(r -> r.get(List.class))
                .map(l -> {
                    final long countryXCount = parseCountryXCount(l, countryX);
                    final long countryYCount = parseCountryYCount(l, countryY);

                    return new LdbcQuery3Result(
                            parseId(toMap(l.get(0))),
                            parseStringValue(toMap(l.get(1)), "firstName"),
                            parseStringValue(toMap(l.get(2)), "lastName"),
                            countryXCount,
                            countryYCount,
                            countryXCount + countryYCount
                    );
                })
                .collect(toList());

        resultReporter.report(0, endResult, operation);
    }
}
