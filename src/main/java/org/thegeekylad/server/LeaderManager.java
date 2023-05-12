package org.thegeekylad.server;

import gash.grpc.route.server.Engine;
import org.thegeekylad.model.RecordQuery;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class LeaderManager {
    // ----------------------------------------------------
    // tables
    // ----------------------------------------------------
    Map<String, RecordQuery> initiatedQueriesMap = new HashMap<>();
    // ----------------------------------------------------
    // handlers
    // ----------------------------------------------------
    LstHandler lstHandler = new LstHandler();
    CpuHandler cpuHandler = new CpuHandler();
    EtlHandler etlHandler = new EtlHandler();
    FndHandler fndHandler = new FndHandler();
    // ----------------------------------------------------
    ServerManager serverManager;
    String csvText;  // huge data loaded onto memory

    public LeaderManager(ServerManager serverManager) {
        this.serverManager = serverManager;
    }

    Route getNewQuery(QueryType queryType, String data, Integer portDestination) {
        String queryId = UUID.randomUUID().toString();
        return getNewQuery(queryId, queryType, data, portDestination == null ? 0 : portDestination);
    }

    Route getNewQuery(String queryId, QueryType queryType, String data, Integer portDestination) {
        RecordQuery recordQuery = new RecordQuery(queryId, queryType, 0);
        initiatedQueriesMap.putIfAbsent(queryId, recordQuery);
        return MessageProcessor.Qry.getMessage(queryId, queryType, data, portDestination);
    }

    void initLeader() {
        // initiate a server list query
        serverManager.sendMessage(getNewQuery(QueryType.LST, "", 0));
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

                // initiate a cpu utilization query
                serverManager.sendMessage(getNewQuery(QueryType.CPU, "", 0));

                postProcessingAfterResponse(incomingResponse);
            }

            return;
        }

        // response is for free cpu query
        if (queryType == QueryType.CPU) {
            serverManager.loggerLeader.log("Received a CPU utilization response.");
            cpuHandler.freeCpuMap.put((int) incomingResponse.getOrigin(), Integer.parseInt(MessageProcessor.Res.getData(incomingResponse)));
            if (isLastResponse(recordQuery)) {

                // servers' cpu utilization received
                cpuHandler.handleResponse();

                File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
                if (outputCsvFile.exists()) {
                    // this is a re-election - data is already everywhere - skip etl, stay smart
                    serverManager.loggerLeader.log("Skipping ETL since data is already everywhere. Redistributing though ...");
                    doDistribution();
                }
                else
                    // time for etl
                    doEtl();

                postProcessingAfterResponse(incomingResponse);
            }

            return;
        }

        // response is for etl query
        if (queryType == QueryType.ETL) {
            serverManager.loggerLeader.log("Received a ETL response: " + incomingResponse.getOrigin());
            if (isLastResponse(recordQuery)) {
                serverManager.loggerLeader.log("Got all ETL responses!");

                // servers' cpu utilization received
                etlHandler.handleResponse();

                // tell everyone how much of data they'd work on
                doDistribution();

                postProcessingAfterResponse(incomingResponse);
            }
        }

        // response is for distribution query (usually the last)
        if (queryType == QueryType.DST) {
            serverManager.loggerLeader.log("Received a DST response: " + incomingResponse.getOrigin());
            if (isLastResponse(recordQuery)) {
                serverManager.loggerLeader.log("Got all DST responses!");

                // TODO the first thing once the server is free
                doRunSampleQuery();

                postProcessingAfterResponse(incomingResponse);
            }
        }

        // response is for server list query
        if (queryType == QueryType.FND) {
            serverManager.loggerLeader.log("Received real-world response data from some server.");
            serverManager.loggerLeader.log(MessageProcessor.getPayload(incomingResponse));
            fndHandler.addMoreCsvContent(Helper.stringBytesToString(MessageProcessor.Res.getData(incomingResponse)));
            if (isLastResponse(recordQuery)) {

                // all response data from all servers received
                fndHandler.handleResponse();

                postProcessingAfterResponse(incomingResponse);
            }

            return;
        }

        // "request" is from external source
        if (queryType == QueryType.EXT) {
            serverManager.loggerLeader.log("Received real-world request. Buckle up, lets go ...");

            // TODO process this real-world request

            return;
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
        serverManager.loggerWarning.log("Reading big CSV ...");
        csvText = Helper.csvToString(csvFile);  // cache csv in memory - biiiiig data!!
        serverManager.loggerWarning.log("CSV read done.");
        String csvBytesString = Helper.stringToStringBytes(csvText);

        // cache on disk on leader as well - what if tomorrow this leader becomes a member?
        File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
        Helper.stringToCsv(csvBytesString, outputCsvFile);

        // cache in memory for leader as well
//        serverManager.csvRecordsList.addAll(Arrays.asList(Helper.stringBytesToString(csvBytesString).split("\n")));

        String queryId = UUID.randomUUID().toString();  // all queries must use the same uuid
        for (Integer portServer : lstHandler.setServers) {
            // TODO replication is okay but how much of data will each server process?
            serverManager.loggerWarning.log("Sending ETL streams to other nodes ...");
            serverManager.sendMessage(getNewQuery(queryId, QueryType.ETL, csvBytesString, portServer));
        }
    }

    void doDistribution() {
        serverManager.loggerLeader.log("Init DST ...");

        // that's because this node was elected as leader in re-election
        File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
        if (csvText == null) {
            serverManager.loggerWarning.log("Reading big CSV ...");
            csvText = Helper.csvToString(outputCsvFile);
            serverManager.loggerWarning.log("CSV read done.");
        }

        String[] countCsvRecords = csvText.trim().split("\n");
        LinkedBlockingDeque<String> queueCsvRecords = new LinkedBlockingDeque<>(Arrays.asList(countCsvRecords));

        int countTotalCores = 0;
        for (Map.Entry<Integer, Integer> cpuInfoEntry : cpuHandler.freeCpuMap.entrySet()) {
            int countCores = cpuInfoEntry.getValue();

            countTotalCores += countCores;
        }

        int[] recordsPerServer = new int[cpuHandler.freeCpuMap.size()];
        int i = 0, countCsvRecordsVisited = 0;
        for (Map.Entry<Integer, Integer> cpuInfoEntry : cpuHandler.freeCpuMap.entrySet()) {
            int countCores = cpuInfoEntry.getValue();

            int r = (int) ((countCores * 1.0 / countTotalCores) * countCsvRecords.length);
            countCsvRecordsVisited += r;

            recordsPerServer[i++] = r;
        }
        int d = countCsvRecords.length - countCsvRecordsVisited;
        recordsPerServer[0] += d;

        serverManager.loggerLeader.log("Total CSV records: " + countCsvRecords.length);
        serverManager.loggerLeader.log("Total CSV records touched: " + countCsvRecordsVisited);

        i = 0;
        countCsvRecordsVisited = 0;
        String queryId = UUID.randomUUID().toString();  // all queries must use the same uuid
        for (Integer portServer : lstHandler.setServers) {
            String range = countCsvRecordsVisited + "-";
            countCsvRecordsVisited += recordsPerServer[i++] - 1;
            range += countCsvRecordsVisited;

            countCsvRecordsVisited++;

            serverManager.loggerLeader.log("\tAllocating to " + portServer + ": " + range);

            serverManager.sendMessage(getNewQuery(queryId, QueryType.DST, range, portServer));
        }
    }

    void doRunSampleQuery() {
        serverManager.loggerLeader.log("Ran a sample query. Lets see how this goes ...");
        String payload = "01/01/2018:01/03/2018:";
        serverManager.sendMessage(getNewQuery(QueryType.FND, payload, null));
    }

    private String getCsvStringForRange(LinkedBlockingDeque<String> queueCsvRecords, int count) {
        StringBuilder csvText = new StringBuilder();
        for (int i = 0; i < count; i++)
            csvText.append(queueCsvRecords.pollFirst()).append("\n");
        return csvText.toString();
    }

    // ----------------------------------------------------
    // lst case handler
    // ----------------------------------------------------
    class LstHandler {
        Set<Integer> setServers = new HashSet<>();

        void handleResponse() {
            serverManager.loggerLeader.log("Discovered these servers in the ring:");
            for (Integer server : setServers)
                serverManager.loggerLeader.log(String.valueOf(server));
        }
    }

    // ----------------------------------------------------
    // cpu case handler
    // ----------------------------------------------------
    class CpuHandler {
        Map<Integer, Integer> freeCpuMap = new HashMap<>();

        void handleResponse() {
            serverManager.loggerLeader.log("Here is the core count info:");
            for (Map.Entry<Integer, Integer> entry : freeCpuMap.entrySet())
                serverManager.loggerLeader.log(entry.getKey() + ": " + entry.getValue() + " cores.");
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

    // ----------------------------------------------------
    // fnd case handler
    // ----------------------------------------------------
    class FndHandler {
        String csvString = "";
        void addMoreCsvContent(String csvString) {
            this.csvString += "\n" + csvString;
        }
        void handleResponse() {
            serverManager.loggerLeader.log("Real-world query fulfilled. Good show");
        }
    }
}
