package org.thegeekylad.server;

import org.thegeekylad.model.RecordQuery;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.io.File;
import java.util.*;

public class LeaderManager {
    // ----------------------------------------------------
    // tables
    // ----------------------------------------------------
    Map<String, RecordQuery> initiatedQueriesMap = new HashMap<>();
    // ----------------------------------------------------
    // handlers
    // ----------------------------------------------------
    LstHandler lstHandler = new LstHandler();
    DskHandler dskHandler = new DskHandler();
    EtlHandler etlHandler = new EtlHandler();
    // ----------------------------------------------------
    ServerManager serverManager;

    public LeaderManager(ServerManager serverManager) {
        this.serverManager = serverManager;
    }

    Route getNewQuery(QueryType queryType, String data) {
        String queryId = UUID.randomUUID().toString();

        RecordQuery recordQuery = new RecordQuery(queryId, queryType, 0);
        initiatedQueriesMap.put(queryId, recordQuery);
        return MessageProcessor.Qry.getMessage(queryId, queryType, data);
    }

    void initLeader() {
        // initiate a server list query
        serverManager.sendMessage(getNewQuery(QueryType.LST, ""));
    }

    // leader stuff - members, stay away

    void processIncomingResponse(Route incomingResponse) {
        serverManager.loggerLeader.log("Leader taking over ...");
        incrementCountForResponse(incomingResponse);

        // process this response
        String queryId = MessageProcessor.Res.getQueryId(incomingResponse);
        RecordQuery recordQuery = initiatedQueriesMap.get(queryId);
        QueryType queryType = recordQuery.type;

        // response is for server list query
        if (queryType == QueryType.LST) {
            serverManager.loggerLeader.log("Received a server list response.");
            lstHandler.setServers.add(Integer.parseInt(MessageProcessor.Res.getData(incomingResponse)));
            if (isLastResponse(recordQuery)) {

                // list of servers received
                lstHandler.handleResponse();

                // initiate a disk utilization query
                serverManager.sendMessage(getNewQuery(QueryType.DSK, ""));

                postProcessingAfterResponse(incomingResponse);
            }

            return;
        }

        // response is for disk space query
        if (queryType == QueryType.DSK) {
            serverManager.loggerLeader.log("Received a disk space response.");
            dskHandler.diskUsageMap.put((int) incomingResponse.getOrigin(), Integer.parseInt(MessageProcessor.Res.getData(incomingResponse)));
            if (isLastResponse(recordQuery)) {

                // servers' disk utilization received
                dskHandler.handleResponse();

                // time for etl
                doEtl();

                postProcessingAfterResponse(incomingResponse);
            }

            return;
        }

        // response is for etl query
        if (queryType == QueryType.ETL) {
            serverManager.loggerLeader.log("Received a ETL response.");
            if (isLastResponse(recordQuery)) {
                serverManager.loggerLeader.log("Got all ETL responses!");

                // servers' disk utilization received
                etlHandler.handleResponse();

                postProcessingAfterResponse(incomingResponse);
            }
        }
    }

    boolean isLastResponse(RecordQuery recordQuery) {
        return recordQuery.countResponses == serverManager.countServers - 1;
    }

    void postProcessingAfterResponse(Route incomingResponse) {
        String queryId = MessageProcessor.Res.getQueryId(incomingResponse);
        initiatedQueriesMap.remove(queryId);  // remove this query as it has been discarded
    }

    void incrementCountForResponse(Route incomingResponse) {
        String queryId = MessageProcessor.Res.getQueryId(incomingResponse);

        // super fatal! there's a response for a query that has been done and dusted with
        if (!initiatedQueriesMap.containsKey(queryId)) {
            serverManager.loggerError.log("Super fatal: Response for query that was already discarded. Some server took too long to process!");
            return;
        }

        // increment responses count
        RecordQuery queryForResponse = initiatedQueriesMap.get(queryId);
        queryForResponse.countResponses++;
        initiatedQueriesMap.put(queryId, queryForResponse);
    }

    void doEtl() {
        serverManager.loggerLeader.log("Init ETL ...");

        // TODO there's actually no csv file stored locally - everything comes form an external agent
        File csvFile = new File(Constants.PATH_CSV_FILE + "/parking-violations.csv");
        String csvBytesString = Helper.csvToString(csvFile);

        serverManager.sendMessage(getNewQuery(QueryType.ETL, csvBytesString));
    }

    // ----------------------------------------------------
    // lst case handler
    // ----------------------------------------------------
    class LstHandler {
        Set<Integer> setServers = new HashSet<>();

        void handleResponse() {
            serverManager.loggerLeader.log("Discovered these servers in the ring:");
            for (Integer server : setServers)
                serverManager.loggerLeader.log(server + ":");
        }
    }

    // ----------------------------------------------------
    // dsk case handler
    // ----------------------------------------------------
    class DskHandler {
        Map<Integer, Integer> diskUsageMap = new HashMap<>();

        void handleResponse() {
            serverManager.loggerLeader.log("Here is the individual server disk utilization info:");
            for (Map.Entry<Integer, Integer> entry : diskUsageMap.entrySet())
                serverManager.loggerLeader.log(entry.getKey() + ": " + entry.getValue() + "GB free.");
        }
    }

    // ----------------------------------------------------
    // etl case handler
    // ----------------------------------------------------
    class EtlHandler {
        void handleResponse() {
            serverManager.loggerLeader.log("ETL fulfilled. Good show");
        }
    }
}
