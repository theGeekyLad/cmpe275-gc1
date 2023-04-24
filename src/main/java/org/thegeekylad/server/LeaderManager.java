package org.thegeekylad.server;

import org.thegeekylad.model.RecordQuery;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LeaderManager {
    // ----------------------------------------------------
    // tables
    // ----------------------------------------------------
    Map<String, RecordQuery> initiatedQueriesMap = new HashMap<>();
    // ----------------------------------------------------
    ServerManager serverManager;
    DskHandler dskHandler = new DskHandler();

    public LeaderManager(ServerManager serverManager) {
        this.serverManager = serverManager;
    }

    void initLeader() {
        String queryId = UUID.randomUUID().toString();

        // initiate a disk space query
        RecordQuery recordQuery = new RecordQuery(queryId, QueryType.DSK, 0);
        initiatedQueriesMap.put(queryId, recordQuery);
        serverManager.sendMessage(MessageProcessor.Qry.getQryMessage(queryId, QueryType.DSK));
    }

    // leader stuff - members, stay away

    void processIncomingResponse(Route incomingResponse) {
        serverManager.loggerLeader.log("Leader taking over ...");
        incrementCountForResponse(incomingResponse);

        // process this response
        String queryId = MessageProcessor.Res.getQueryId(incomingResponse);
        RecordQuery recordQuery = initiatedQueriesMap.get(queryId);
        QueryType queryType = recordQuery.type;

        // record disk spaces
        if (queryType == QueryType.DSK) {
            serverManager.loggerLeader.log("Received a disk space response.");
            dskHandler.diskUsageMap.put((int) incomingResponse.getOrigin(), Integer.parseInt(MessageProcessor.Res.getData(incomingResponse)));
            if (isLastResponse(recordQuery)) {
                serverManager.loggerLeader.log("This is the last disk space response.");
                dskHandler.handleResponse();
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

    // ----------------------------------------------------
    // dsk case handler
    // ----------------------------------------------------
    class DskHandler {
        Map<Integer, Integer> diskUsageMap = new HashMap<>();

        void handleResponse() {
            for (Map.Entry<Integer, Integer> entry : diskUsageMap.entrySet())
                serverManager.loggerLeader.log(entry.getKey() + "|" + entry.getValue());

            // TODO something else with disk spaces
        }
    }
}
