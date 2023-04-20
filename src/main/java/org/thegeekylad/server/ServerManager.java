package org.thegeekylad.server;

import com.google.protobuf.ByteString;
import gash.grpc.route.server.Engine;
import gash.grpc.route.server.Link;
import gash.grpc.route.server.RouteServerImpl;
import org.thegeekylad.Router;
import org.thegeekylad.util.ConsoleColors;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.MyLogger;
import org.thegeekylad.util.constants.Constants;
import org.thegeekylad.util.constants.MessageType;
import route.Route;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

public class ServerManager {
    // ----------------------------------------------------
    // routes
    // ----------------------------------------------------
    private Route incomingMessage;
    private Route messageHbt;
    // ----------------------------------------------------
    // locks
    // ----------------------------------------------------
    private final Object lockMessageHbt = new Object();
    // ----------------------------------------------------
    // loggers
    // ----------------------------------------------------
    private MyLogger myLogger;
    private MyLogger loggerHbt;
    private MyLogger loggerSuccess = new MyLogger(getClass().getName(), ConsoleColors.GREEN_BOLD);
    private MyLogger loggerRecurring = new MyLogger(getClass().getName(), ConsoleColors.PURPLE);
    private MyLogger loggerWarning = new MyLogger(getClass().getName(), ConsoleColors.YELLOW);
    // ----------------------------------------------------
    // threads
    // ----------------------------------------------------
    private Thread threadSender;
    private Thread threadHbtIncoming;
    private Thread threadHbt;
    // ----------------------------------------------------
    // counts
    // ----------------------------------------------------
    private int countRequests;
    private int countRequestsElc;
    private int countHbtMisses;
    // ----------------------------------------------------
    // links
    // ----------------------------------------------------
    private final Link link = Engine.getInstance().links.get(0);
    private Long linkPrev = null;
    // ----------------------------------------------------
    private Integer portLeader;
    private Date dateLastSentElc;
    private RouteServerImpl routeServer;
    final private LinkedBlockingDeque<Route> queueElcMessages;
    private int serverPriority;  // TODO make something more meaningful

    // ----------------------------------------------------

    public ServerManager() {
        countRequests = 0;
        countRequestsElc = 0;
        portLeader = null;
        queueElcMessages = new LinkedBlockingDeque<>();

        myLogger = new MyLogger(getClass().getName(), ConsoleColors.GREEN);
        loggerHbt = new MyLogger(getClass().getName(), ConsoleColors.YELLOW);

        serverPriority = new Random().nextInt(10);

        myLogger.log("Service manager created. Priority: " + serverPriority);
    }

    public ServerManager(RouteServerImpl routeServer) {
        this();
        this.routeServer = routeServer;
    }

    // ----------------------------------------------------

    private String getMessageType(Route msg) {
        String payload = new String(msg.getPayload().toByteArray());
        return payload.split("#")[0];
    }

    private Date getTimestamp(Route msgElc) {
        String payload = new String(msgElc.getPayload().toByteArray());
        return Helper.getDate(payload.split("#")[1]);
    }

    private int getPort(Route msg) {
        String payload = new String(msg.getPayload().toByteArray());
        return Integer.parseInt(payload.split("#")[3]);
    }

    private int getPriority(Route msg) {
        String payload = new String(msg.getPayload().toByteArray());
        return Integer.parseInt(payload.split("#")[2]);
    }

    private ByteString ack(route.Route msg) {
        // TODO complete processing
        final String blank = "accepted";
        byte[] raw = blank.getBytes();

        return ByteString.copyFrom(raw);
    }

    private void processEnqueueElc(Route msg) {
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

    private boolean isDead(Thread thread) {
        return thread == null || !thread.isAlive();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    public void sendMessage(Route msg) {
        String messageTypeName = getMessageType(msg);

        if (messageTypeName.equals(MessageType.HBT.name())) {
            Router.getInstance().run(link.getPort(), msg);
            loggerRecurring.log("Heartbeat sent.");
            return;
        }

        if (messageTypeName.equals(MessageType.RNG.name())) {
            Router.getInstance().run(link.getPort(), msg);
            loggerWarning.log("Ring restoration message sent.");
            return;
        }

        if (messageTypeName.equals(MessageType.ELC.name())) {
            myLogger.log("Enqueued message.");
            processEnqueueElc(msg);

            if (isDead(threadSender)) {
                threadSender = new Thread(() -> {
                    while (true) {
                        try {
                            synchronized (queueElcMessages) {
                                // stopping case
                                if (queueElcMessages.isEmpty()) break;

//                                myLogger.log("Pending: " + queueElcMessages.size());

                                Router.getInstance().run(link.getPort(), queueElcMessages.peek());
                                queueElcMessages.poll();

                                loggerSuccess.log("Connected. Sent.");
                            }
                        } catch (Exception e) {
                            Router.getInstance().close();
                            sleep(1000);
                            myLogger.log("...");
                        }
                    }
                    myLogger.log("Thread stopped.");
                });
                threadSender.start();
            }

            return;
        }
    }

    public int getMyPriority() {
        return serverPriority;  // TODO something meaningful
    }

    public void processIncomingMessage(Route incomingMessage) {
        this.incomingMessage = incomingMessage;
        countRequests++;

        myLogger.log("Inside manager.");

        // TODO requestCount can be >= 2, even at the onset, if there are bombarding incoming requests
        if (getMessageType(incomingMessage).equals(MessageType.ELC.name())) {
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

                sendMessage(getElcMessage(incomingMessage));
            }

            // this elc is the same as the one initiated by me
            else {
                myLogger.log("BINGO! Got back my ELC.");

                countRequestsElc++;

                switch (countRequestsElc) {
                    case 1:
                        // this is the first time you got an elc request: might be leader?
                        myLogger.log("First time. Spreading the word.");
                        sendMessage(getElcMessage(incomingMessage));
                        break;

                    case 2:
                        // this is the second time
                        myLogger.log("Second time. Stopping everything. Leader has been elected.");
                        portLeader = getPort(incomingMessage);
                        loggerSuccess.log("Leader elected: " + portLeader);

                        // start sending heartbeats
                        if (isDead(threadHbt))
                            (threadHbt = new Thread(this::startSendingHeartbeats)).start();

                        if (incomingMessage.getOrigin() == Engine.getInstance().serverPort)
                            myLogger.log("Back at me. I started everything. Stopping.");
                        else
                            sendMessage(getElcMessage(incomingMessage));  // pass on the update
                        break;
                }
            }
            return;
        }

        // this is a hbt type message
        if (getMessageType(incomingMessage).equals(MessageType.HBT.name())) {
            synchronized (lockMessageHbt) {
                messageHbt = incomingMessage;
            }
            // start expecting heart beats: happens only once in a lifetime
            if (isDead(threadHbtIncoming))
                (threadHbtIncoming = new Thread(this::startExpectingHeartbeats)).start();

            return;
        }

        // this is a ring completion request
        if (getMessageType(incomingMessage).equals(MessageType.RNG.name())) {

            loggerWarning.log("Ring restoration request received.");

            // this is for me - update link
            if (link.getPort() == getRngOldPort(incomingMessage)) {
                loggerWarning.log("Ring restoration request is for me. Replacing link ...");
                link.setPort((int) incomingMessage.getOrigin());
            }

            else {
                loggerWarning.log("Passing it on.");
                sendMessage(incomingMessage);  // not for me, just pass it on
            }
        }

        // TODO - enqueue work - do some processing on incomingMessage or your own

//            Route.Builder messageForward = Route.newBuilder();
//            messageForward.setId(Engine.getInstance().getNextMessageID());
//            messageForward.setOrigin(Engine.getInstance().getServerID());
//            messageForward.setDestination(link.getPort());
//            messageForward.setPath("");
//            messageForward.setPayload(ByteString.copyFrom("TODO insert payload.".getBytes()));
    }

    private void startExpectingHeartbeats() {
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

    private void startSendingHeartbeats() {
        while (true) {
            sleep(1000);
            try {
                sendMessage(getHbtMessage());
            } catch (Exception e) {  // what if the next server is dead?
                // ignore
            }
        }
    }

    private int getRngOldPort(Route msg) {
        String payload = new String(msg.getPayload().toByteArray());
        return Integer.parseInt(payload.split("#")[1]);
    }

    private Route getRngMessage() {
        String payload = MessageType.RNG.name() + "#" + linkPrev;
        return Route.newBuilder()
                .setId(0)
                .setOrigin(Engine.getInstance().serverPort)
                .setDestination(0)
                .setPath("")
                .setPayload(ByteString.copyFrom(payload.getBytes()))
                .build();
    }

    private Route getHbtMessage() {
        return Route.newBuilder()
                .setId(0)
                .setOrigin(Engine.getInstance().serverPort)
                .setDestination(link.getPort())
                .setPath("")
                .setPayload(ByteString.copyFrom(MessageType.HBT.name().getBytes()))
                .build();
    }

    public Route getElcMessage(Route incomingMessage) {
        Route.Builder msg = Route.newBuilder();
        msg.setId(0);
        msg.setDestination(0);
        msg.setPath("");

        int myPriority = getMyPriority();

        if (incomingMessage == null) {
            myLogger.log("Sending ELC before hearing from anyone! Let's go.");
            msg.setOrigin(Engine.getInstance().serverPort);
            msg.setPayload(
                    ByteString.copyFrom(
                            new StringBuilder()
                                    .append(MessageType.ELC.name())
                                    .append("#")
                                    .append(Helper.getTimestampString(dateLastSentElc = new Date()))
                                    .append("#")
                                    .append(myPriority)
                                    .append("#")
                                    .append(myPriority)
                                    .toString()
                                    .getBytes()
                    )
            );
        } else {
            int incomingPriority = getPriority(incomingMessage);
            msg.setOrigin(incomingMessage.getOrigin());
            msg.setPayload(
                    ByteString.copyFrom(
                            new StringBuilder()
                                    .append(MessageType.ELC.name())
                                    .append("#")
                                    .append(Helper.getTimestampString(getTimestamp(incomingMessage)))
                                    .append("#")
                                    .append(Math.max(myPriority, incomingPriority))
                                    .append("#")
                                    .append(myPriority > incomingPriority ? Engine.getInstance().serverPort : getPort(incomingMessage))
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
