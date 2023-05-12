package org.thegeekylad.server;

import gash.grpc.route.server.Engine;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class Worker implements Runnable {
    LinkedBlockingDeque<Route> queueWork;
    private Thread thread;
    private ServerManager serverManager;

    public Worker(ServerManager serverManager) {
        this.serverManager = serverManager;
        queueWork = new LinkedBlockingDeque<>();
    }

    void enqueueWork(Route msg) {
        serverManager.loggerWarning.log("More work to do.");

        queueWork.addFirst(msg);

        if (Helper.isDead(thread))
            (thread = new Thread(this)).start();
    }

    @Override
    public void run() {
        serverManager.loggerWarning.log("Worker running ...");
        while (!queueWork.isEmpty()) {
            Route msg = queueWork.poll();

            // this is a server list query request - tell the leader you're here
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.LST.name())) {
                serverManager.loggerResponse.log("Response for LST built. Enqueueing send ...");
                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        String.valueOf(Engine.getInstance().serverPort)));
                return;
            }

            // this is a cpu utilization query request - serve it
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.CPU.name())) {
                serverManager.loggerResponse.log("Response for CPU built. Enqueueing send ...");
                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        String.valueOf(serverManager.getFreeCpuInfo())));
                return;
            }

            // this is an etl request - get em data, lets go
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.ETL.name())) {
                serverManager.loggerWarning.log("Ingesting ETL data ...");

                // cache on disk
                String csvBytesString = MessageProcessor.Qry.getData(msg);
                File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
                Helper.stringToCsv(csvBytesString, outputCsvFile);

                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        ""));
            }

            // this is a dst request - save the range
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.DST.name())) {
                serverManager.loggerWarning.log("Saving my workable range ...");

                serverManager.range = MessageProcessor.Qry.getData(msg).split("-");

                File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
                String[] csvRecords = Helper.csvToString(outputCsvFile).split("\n");
                serverManager.loggerWarning.log("Range: " + Arrays.toString(serverManager.range));
                serverManager.loggerWarning.log("Length: " + csvRecords.length);
                serverManager.csvRecords = Arrays.asList(csvRecords).subList(Integer.parseInt(serverManager.range[0]), Integer.parseInt(serverManager.range[1]) + 1).toArray(new String[] {});

                serverManager.loggerWarning.log("\tSaved new data to work with!");

                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        String.valueOf(serverManager.getFreeCpuInfo())));
            }

            // this is a real query - look for data NOW !!!
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.FND.name())) {
                try {
                    serverManager.loggerWarning.log("Real-world query. Searching ...");

                    String payload = MessageProcessor.Qry.getData(msg);
                    String[] findQueryParts = payload.split(":");
                    Date dateFrom = new SimpleDateFormat("dd/MM/yyyy").parse(findQueryParts[0]);
                    Date dateTo = new SimpleDateFormat("dd/MM/yyyy").parse(findQueryParts[1]);

                    List<String> lines = new ArrayList<>();
                    serverManager.loggerWarning.log("Searching ...");

                    // TODO end range is WRONG
                    for (int i = Integer.parseInt(serverManager.range[0]); i < serverManager.csvRecords.length; i++) {
                        Date dateRecord = new SimpleDateFormat("dd/MM/yyyy").parse(serverManager.csvRecords[i].split(",")[4]);
                        if (dateRecord.equals(dateFrom) || dateRecord.equals(dateTo) || dateRecord.after(dateRecord) && dateRecord.before(dateTo))
                            lines.add(serverManager.csvRecords[i]);
                    }
                    serverManager.loggerWarning.log("Search done.");

                    serverManager.sendMessage(MessageProcessor.Res.getMessage(
                            MessageProcessor.Qry.getId(msg),
                            String.join("\n", lines)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        serverManager.loggerWarning.log("Worker stopped.");
    }
}
