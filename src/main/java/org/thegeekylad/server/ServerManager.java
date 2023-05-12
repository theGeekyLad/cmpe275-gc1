package org.thegeekylad.server;

import com.google.protobuf.ByteString;
import gash.grpc.route.server.Engine;
import gash.grpc.route.server.Link;
import gash.grpc.route.server.RouteServerImpl;
import org.apache.commons.io.FileSystemUtils;
import org.thegeekylad.Router;
import org.thegeekylad.server.processor.MessageProcessor;
import org.thegeekylad.util.ConsoleColors;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.MyLogger;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class ServerManager {
    // ----------------------------------------------------
    // managers
    // ----------------------------------------------------
    LeaderManager leaderManager = new LeaderManager(this);
    // ----------------------------------------------------
    // routes
    // ----------------------------------------------------
    Route incomingMessage;
    Route messageHbt;
    // ----------------------------------------------------
    // locks
    // ----------------------------------------------------
    final Object lockMessageHbt = new Object();
    // ----------------------------------------------------
    // loggers
    // ----------------------------------------------------
    MyLogger myLogger;
    MyLogger loggerHbt;
    MyLogger loggerSuccess = new MyLogger(getClass().getName(), ConsoleColors.GREEN_BOLD);
    MyLogger loggerRecurring = new MyLogger(getClass().getName(), ConsoleColors.PURPLE);
    MyLogger loggerWarning = new MyLogger(getClass().getName(), ConsoleColors.YELLOW);
    MyLogger loggerQuery = new MyLogger(getClass().getName(), ConsoleColors.CYAN);
    MyLogger loggerResponse = new MyLogger(getClass().getName(), ConsoleColors.CYAN_UNDERLINED);
    MyLogger loggerError = new MyLogger(getClass().getName(), ConsoleColors.RED_BACKGROUND);
    MyLogger loggerLeader = new MyLogger(getClass().getName(), ConsoleColors.BLUE);
    // ----------------------------------------------------
    // threads
    // ----------------------------------------------------
    Thread threadSender;
    Thread threadHbtIncoming;
    Thread threadHbt;
    Worker worker = new Worker(this);
    // ----------------------------------------------------
    // counts
    // ----------------------------------------------------
    int countRequests;
    int countRequestsElc;
    int countHbtMisses;
    int countServers;
    // ----------------------------------------------------
    // links
    // ----------------------------------------------------
    final Link link = Engine.getInstance().links.get(0);
    Long linkPrev = null;
    // ----------------------------------------------------
    // queues
    // ----------------------------------------------------
    final LinkedBlockingDeque<Route> queueElcMessages = new LinkedBlockingDeque<>();
    final LinkedBlockingDeque<Route> queueMessages = new LinkedBlockingDeque<>();
    // ----------------------------------------------------
    // data
    // ----------------------------------------------------
    String[] csvRecords;  // only the part that needs to be used
    // ----------------------------------------------------
    Integer portLeader;
    Date dateLastSentElc;
    RouteServerImpl routeServer;
    int serverPriority;  // TODO make something more meaningful
    boolean elcInitiator;
    // ----------------------------------------------------

    public ServerManager() {
        countRequests = 0;
        countRequestsElc = 0;
        portLeader = null;

        myLogger = new MyLogger(getClass().getName(), ConsoleColors.GREEN);
        loggerHbt = new MyLogger(getClass().getName(), ConsoleColors.YELLOW);

        serverPriority = new Random().nextInt(10);

        // delete old cached csv data
        File outputCsvFile = new File(Constants.PATH_CSV_FILE_OUTPUT + "/" + Engine.getInstance().serverPort + "-csv.csv");
        outputCsvFile.delete();

        myLogger.log("Service manager created. Priority: " + serverPriority);
    }

    public ServerManager(RouteServerImpl routeServer) {
        this();
        this.routeServer = routeServer;
    }

    // ----------------------------------------------------

    Date getTimestamp(Route msgElc) {
        String payload = MessageProcessor.getPayload(msgElc);
        return Helper.getDate(payload.split("#")[1]);
    }

    int getPort(Route msg) {
        String payload = MessageProcessor.getPayload(msg);
        return Integer.parseInt(payload.split("#")[3]);
    }

    int getPriority(Route msg) {
        String payload = MessageProcessor.getPayload(msg);
        return Integer.parseInt(payload.split("#")[2]);
    }

    ByteString ack(route.Route msg) {
        // TODO complete processing
        final String blank = "accepted";
        byte[] raw = blank.getBytes();

        return ByteString.copyFrom(raw);
    }

    void processEnqueueElc(Route msg) {
        synchronized (queueElcMessages) {
            if (queueElcMessages.peekFirst() == null) {
                queueElcMessages.add(msg);
                myLogger.log("Queue: Directly enqueued.");
            } else if (getTimestamp(msg).before(getTimestamp(queueElcMessages.peekFirst()))) {
                queueElcMessages.pollFirst();
                queueElcMessages.addFirst(msg);
                myLogger.log("Queue: Updated newer ELC with older.");
            }
        }
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    public void sendMessage(Route msg) {
        String messageTypeName = MessageProcessor.getType(msg);

        // send heartbeats
        if (messageTypeName.equals(MessageProcessor.Type.HBT.name())) {
            Router.getInstance().run(link.getPort(), msg);
            loggerRecurring.log("Heartbeat sent.");
            return;
        }

        // send ring restoration
        if (messageTypeName.equals(MessageProcessor.Type.RNG.name())) {
            Router.getInstance().run(link.getPort(), msg);
            loggerWarning.log("Ring restoration message sent.");
            return;
        }

        // send election message
        if (messageTypeName.equals(MessageProcessor.Type.ELC.name())) {
            myLogger.log("Enqueued message.");

            // get rid of newer elcs in the queue
            processEnqueueElc(msg);

            if (Helper.isDead(threadSender))
                (threadSender = new Thread(this::startSendingQueuedMessages)).start();

            return;
        }

        // TODO if ELC or RNG is in effect, these shouldn't happen - re-prioritize
        // send other messages
        if (messageTypeName.equals(MessageProcessor.Type.RES.name())
                || messageTypeName.equals(MessageProcessor.Type.QRY.name())) {

            // choose the right logger
            if (messageTypeName.equals(MessageProcessor.Type.RES.name()))
                loggerResponse.log("Enqueued response message.");
            else if (messageTypeName.equals(MessageProcessor.Type.QRY.name()))
                loggerQuery.log("Enqueued query message.");

            queueMessages.addFirst(msg);

            if (Helper.isDead(threadSender))
                (threadSender = new Thread(this::startSendingQueuedMessages)).start();

            return;
        }
    }

    public int getMyPriority() {
        return serverPriority;  // TODO make server priority score meaningful
    }

    public long getFreeCpuInfo() {
        return Runtime.getRuntime().availableProcessors();
    }

    public void processIncomingMessage(Route incomingMessage) {
        this.incomingMessage = incomingMessage;
        countRequests++;

        myLogger.log("Inside manager.");

        // TODO requestCount can be >= 2, even at the onset, if there are bombarding incoming requests
        if (MessageProcessor.getType(incomingMessage).equals(MessageProcessor.Type.ELC.name())) {

            Date dateThisElc = getTimestamp(incomingMessage);

            // this elc message came in before my first was even sent (super rare)
            if (dateLastSentElc == null) {
                myLogger.log("Super rare case just happened!");
                // drop it
                // already handled in RouteServerImp / run()
            }

            // this elc message was generated after mine was sent
            else if (dateThisElc.after(dateLastSentElc)) {
                myLogger.log("Newer ELC. Dropping ...");
                // drop it
            }

            // this elc message was generated before mine was
            else if (dateThisElc.before(dateLastSentElc)) {
                myLogger.log("Older ELC. Considering and forwarding ...");

                dateLastSentElc = dateThisElc;
                countRequestsElc = 1;

                sendMessage(getElcMessage(incomingMessage, true));
            }

            // this elc is the same as the one initiated by me
            else {
                myLogger.log("BINGO! Got back my ELC.");

                countRequestsElc++;

                switch (countRequestsElc) {
                    case 1 -> {
                        // this is the first time you got an elc request: might be leader?
                        myLogger.log("First time. Spreading the word.");
                        sendMessage(getElcMessage(incomingMessage, false));
                    }
                    case 2 -> {
                        // this is the second time
                        myLogger.log("Second time. Stopping everything. Leader has been elected.");
                        portLeader = getPort(incomingMessage);
                        loggerSuccess.log("Leader elected: " + portLeader);

                        // start sending heartbeats
                        if (Helper.isDead(threadHbt))
                            (threadHbt = new Thread(this::startSendingHeartbeats)).start();

                        if (incomingMessage.getOrigin() == Engine.getInstance().serverPort) {
                            myLogger.log("Back at me. I started everything. Stopping.");
                            countRequestsElc++;
                        }
                        else
                            sendMessage(getElcMessage(incomingMessage, false));  // pass on the update

                        // as a leader, start working on stuff (query, etc.)
                        if (isMeLeader())
                            leaderManager.initLeader();
                    }

                    // members do nothing
                }
            }
            return;
        }

        // this is a hbt type message
        if (MessageProcessor.getType(incomingMessage).equals(MessageProcessor.Type.HBT.name())) {
            synchronized (lockMessageHbt) {
                messageHbt = incomingMessage;
            }
            // start expecting heart beats: happens only once in a lifetime
            if (Helper.isDead(threadHbtIncoming))
                (threadHbtIncoming = new Thread(this::startExpectingHeartbeats)).start();

            return;
        }

        // this is a ring completion request
        if (MessageProcessor.getType(incomingMessage).equals(MessageProcessor.Type.RNG.name())) {

            loggerWarning.log("Ring restoration request received.");

            // this is for me - update link
            if (link.getPort() == getRngOldPort(incomingMessage)) {
                loggerWarning.log("Ring restoration request is for me. Replacing link ...");
                link.setPort((int) incomingMessage.getOrigin());

                // start re-election
                loggerWarning.log("Starting re-election");
                sendMessage(getElcMessage(null, true));  // "true" isn't relevant - can be false too
            } else {
                loggerWarning.log("Passing it on.");
                sendMessage(incomingMessage);  // not for me, just pass it on
            }
            return;
        }

        // this is a query, offload to worker thread
        if (MessageProcessor.getType(incomingMessage).equals(MessageProcessor.Type.QRY.name())) {

            loggerQuery.log("Query received.");

            // do leader stuff as a leader
            if (isMeLeader()) {
                loggerQuery.log("Hey, I'm the leader! Don't query me.");
                return;
            }

            // do something only if this query is for me or generic "0"
            if (incomingMessage.getDestination() == 0 || incomingMessage.getDestination() == Engine.getInstance().serverPort) {

                // etl or dst queries to be offloaded without a thought
                if (MessageProcessor.Qry.getType(incomingMessage).equals(QueryType.ETL.name())
                        || MessageProcessor.Qry.getType(incomingMessage).equals(QueryType.DST.name())) {
                    loggerWarning.log("ETL / DST type of query.");
                    worker.enqueueWork(incomingMessage);
                    return;
                }

                // offload to worker
                worker.enqueueWork(incomingMessage);
            } else
                loggerWarning.log("This query isn't for me, passing it on ...");

            // pass on the query to others
            sendMessage(incomingMessage);

            return;
        }

        // this is a response from another member
        if (MessageProcessor.getType(incomingMessage).equals(MessageProcessor.Type.RES.name())) {
            if (isMeLeader()) {
                loggerResponse.log("I'm the leader and I got the response for my query!");
                leaderManager.processIncomingResponse(incomingMessage);
                return;
            }

            // pass it on - just a response from some other member
            sendMessage(incomingMessage);
        }
    }

    boolean isMeLeader() {
        return portLeader == (int) Engine.getInstance().serverPort;
    }

    void startExpectingHeartbeats() {
        while (true) {
            sleep(Constants.INTERVAL_HBT);

            synchronized (lockMessageHbt) {
                if (messageHbt == null) {
                    countHbtMisses++;
                    if (countHbtMisses == Constants.RETRIES_HBT) {
                        loggerWarning.log("No heart beat in too long. Ring restoration initiated.");
                        sendMessage(getRngMessage());
                    }
                }
                // we're getting dem heartbeats
                else {
                    linkPrev = messageHbt.getOrigin();
                    loggerRecurring.log("Heartbeat received.");
                    messageHbt = null;
                    countHbtMisses = 0;
                }
            }
        }
    }

    void startSendingHeartbeats() {
        while (true) {
            sleep(1000);
            try {
                sendMessage(MessageProcessor.Hbt.getMessage(link.getPort()));
            } catch (Exception e) {  // what if the next server is dead?
                // ignore
            }
        }
    }

    void startSendingQueuedMessages() {
        outer:
        while (true) {
            try {
                synchronized (queueElcMessages) {
                    while (true) {
                        if (!queueElcMessages.isEmpty()) {
                            Router.getInstance().run(link.getPort(), queueElcMessages.peek());
                            queueElcMessages.poll();
                            loggerSuccess.log("Connected. Sent.");
                            continue;
                        }
                        break;  // quit
                    }
                }

                // send pending responses
                while (true) {
                    if (!queueMessages.isEmpty()) {
                        Router.getInstance().run(link.getPort(), queueMessages.poll());
                        loggerQuery.log("Sent a query or response, I don't know.");
                        continue outer;  // check if for pending elcs before sending responses
                    }
                    break;  // quit
                }

                break;  // suspend until a new elc or response message is to be sent
            } catch (Exception e) {
                Router.getInstance().close();
                sleep(1000);
                myLogger.log("...");
            }
        }
        myLogger.log("Thread stopped.");
    }

    int getRngOldPort(Route msg) {
        String payload = MessageProcessor.getPayload(msg);
        return Integer.parseInt(payload.split("#")[1]);
    }

    Route getRngMessage() {
        String payload = MessageProcessor.Type.RNG.name() + "#" + linkPrev;
        return Route.newBuilder()
                .setId(0)
                .setOrigin(Engine.getInstance().serverPort)
                .setDestination(0)
                .setPath("")
                .setPayload(ByteString.copyFrom(payload.getBytes()))
                .build();
    }

    public Route getElcMessage(Route incomingMessage, boolean mustCount) {
        Route.Builder msg = Route.newBuilder()
                .setId(0)
                .setOrigin(Engine.getInstance().serverPort)
                .setDestination(0)
                .setPath("");

        int myPriority = getMyPriority();

        if (incomingMessage == null) {
            myLogger.log("Sending ELC before hearing from anyone! Let's go.");
//            elcInitiator = true;
            msg.setPayload(
                    ByteString.copyFrom(
                            new StringBuilder()
                                    .append(MessageProcessor.Type.ELC.name())
                                    .append("#")
                                    .append(Helper.getTimestampString(dateLastSentElc = new Date()))
                                    .append("#")
                                    .append(myPriority)
                                    .append("#")
                                    .append(Engine.getInstance().serverPort)
                                    .append("#")
                                    .append(1)
                                    .toString()
                                    .getBytes()
                    )
            );
        } else {
            int incomingPriority = getPriority(incomingMessage);
            int incomingServerCount = MessageProcessor.Elc.getCount(incomingMessage);

            msg.setPayload(
                    ByteString.copyFrom(
                            new StringBuilder()
                                    .append(MessageProcessor.Type.ELC.name())
                                    .append("#")
                                    .append(Helper.getTimestampString(getTimestamp(incomingMessage)))
                                    .append("#")
                                    .append(Math.max(myPriority, incomingPriority))
                                    .append("#")
                                    .append(myPriority > incomingPriority ? Engine.getInstance().serverPort : getPort(incomingMessage))
                                    .append("#")
                                    .append(mustCount ? incomingServerCount + 1 : incomingServerCount)
                                    .toString()
                                    .getBytes()
                    )
            );
        }

        return msg.build();
    }

    public Route getAck(Route request) {
        Route.Builder ack = Route.newBuilder();

        ack.setId(Engine.getInstance().getNextMessageID());
        ack.setOrigin(Engine.getInstance().getServerID());
        ack.setDestination(request.getOrigin());
        ack.setPath(request.getPath());
        ack.setPayload(ack(request));

        return ack.build();
    }
}
