package org.thegeekylad.server;

import gash.grpc.route.server.Engine;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.io.File;
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

            // this is a disk space query request - serve it
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.DSK.name())) {
                serverManager.loggerResponse.log("Response for DSK built. Enqueueing send ...");
                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        String.valueOf(serverManager.getDiskFreeSpace())));
                return;
            }

            // this is an etl request - get em data, lets go
            if (MessageProcessor.Qry.getType(msg).equals(QueryType.ETL.name())) {
                serverManager.loggerWarning.log("Ingesting ETL data ...");

                // commit to disk
                String csvBytesString = MessageProcessor.Qry.getData(msg);
                File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
                Helper.stringToCsv(csvBytesString, outputCsvFile);

                serverManager.sendMessage(MessageProcessor.Res.getMessage(
                        MessageProcessor.Qry.getId(msg),
                        String.valueOf(serverManager.getDiskFreeSpace())));
            }
        }
        serverManager.loggerWarning.log("Worker stopped.");
    }
}
