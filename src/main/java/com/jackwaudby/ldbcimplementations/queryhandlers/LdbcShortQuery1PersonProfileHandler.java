package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfile;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcShortQuery1PersonProfileResult;

import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, gender, creation date and the ID of their city of residence.
 */
@Slf4j
@SuppressWarnings("unused")
public class LdbcShortQuery1PersonProfileHandler extends GremlinHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {
    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        final long personId = operation.getPersonIdSQ1();
        final JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        final String queryString = "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Person','id'," + personId + ")." +
                "union(" +
                "valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').unfold()," +
                "out('isLocatedIn').valueMap('id').unfold()" +
                ").fold().toList();" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result";

        int attempts = 0;
        final int retries = getTxnAttempts();
        while (attempts < retries) {
            log.info("Attempt {}", attempts + 1);
            final List<Result> response = request(client, queryString);

            if (toMap(response.get(0).get(List.class).get(0)).containsKey("error")) {
                log.error(parseStringValue(toMap(response.get(0)), "error"));
                attempts = attempts + 1;
            } else {
                final List<?> resultBreakdown = response.get(0).get(List.class);

                try {
                    final LdbcShortQuery1PersonProfileResult ldbcShortQuery1PersonProfileResult = new LdbcShortQuery1PersonProfileResult(
                            parseFirstName(toMap(resultBreakdown.get(3))),
                            parseLastName(toMap(resultBreakdown.get(4))),
                            parsePersonBirthday(toMap(resultBreakdown.get(6))),
                            parsePersonLocationIP(toMap(resultBreakdown.get(2))),
                            parsePersonBrowserUsed(toMap(resultBreakdown.get(1))),
                            parseId(toMap(resultBreakdown.get(7))),
                            parsePersonGender(toMap(resultBreakdown.get(5))),
                            parseCreationDate(toMap(resultBreakdown.get(0)))
                    );
                    resultReporter.report(0, ldbcShortQuery1PersonProfileResult, operation);
                    break;
                } catch (Exception e) {
                    log.error("Unexpected error", e);
                    attempts = attempts + 1;
                }
            }
        }
    }
}
