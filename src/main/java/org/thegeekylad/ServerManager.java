package org.thegeekylad;

import com.google.protobuf.ByteString;
import gash.grpc.route.client.RouteClient;
import gash.grpc.route.server.Engine;
import gash.grpc.route.server.Link;
import gash.grpc.route.server.RouteServerImpl;
import org.thegeekylad.util.Helper;
import org.thegeekylad.util.constants.MessageType;
import route.Route;

import java.util.Date;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

public class ServerManager {
    private Route incomingMessage;
    private Integer portLeader;
    private Date dateLastSentElc;
    private RouteServerImpl routeServer;
    final private LinkedBlockingDeque<Route> queueElcMessages;
    private Thread senderThread;
    private int requestCount;
    private int requestCountElc;
    private int serverPriority;  // TODO make something more meaningful

    // ----------------------------------------------------

    public ServerManager() {
        requestCount = 0;
        requestCountElc = 0;
        portLeader = null;
        queueElcMessages = new LinkedBlockingDeque<>();

        serverPriority = new Random().nextInt(10);

        Engine.log("Service manager created. Priority: " + serverPriority);
    }

    public ServerManager(RouteServerImpl routeServer) {
        this();
        this.routeServer = routeServer;
    }

    // ----------------------------------------------------

    private boolean isMessageElc(Route msg) {
        String payload = new String(msg.getPayload().toByteArray());
        return payload.split("#")[0].equals(MessageType.ELC.name());
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
                Engine.log("\tQueue: Directly enqueued.");
            } else if (getTimestamp(msg).before(getTimestamp(queueElcMessages.peekFirst()))) {
                queueElcMessages.pollFirst();
                queueElcMessages.addFirst(msg);
                Engine.log("\tQueue: Updated newer ELC with older.");
            }
        }
    }

    // ----------------------------------------------------

    public void sendMessage(Route msg) {
        // TODO only if this is an elc type message
        Engine.log("Enqueued message.");
        processEnqueueElc(msg);

        if (senderThread == null || !senderThread.isAlive()) {
            senderThread = new Thread(() -> {
                while (true) {
                    try {
                        synchronized (queueElcMessages) {
                            // stopping case
                            if (queueElcMessages.isEmpty()) break;

                            Engine.log("Pending: " + queueElcMessages.size());

                            Link link = Engine.getInstance().links.get(0);
                            Router.getInstance().run(link.getPort(), queueElcMessages.peek());
                            queueElcMessages.poll();
                            Engine.log("Connected. Sent.");
                        }
                    } catch (Exception e) {
                        Router.getInstance().close();

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                        }

                        Engine.log("Couldn't connect, retrying ...");
                    }
                }
                Engine.log("Thread stopped.");
            });
            senderThread.start();
        }
    }

    public int getMyPriority() {
        return serverPriority;  // TODO something meaningful
    }

    public void processIncomingMessage(Route incomingMessage) {

        synchronized (this) {
            this.incomingMessage = incomingMessage;
            requestCount++;

            Engine.log("\tInside manager.");

            // TODO requestCount can be >= 2, even at the onset, if there are bombarding incoming requests
            if (isMessageElc(incomingMessage)) {
                Date dateThisElc = getTimestamp(incomingMessage);

                // this elc message came in before my first was even sent (super rare)
                if (dateLastSentElc == null) {
                    Engine.log("\tSuper rare case just happened!");
                    // drop it
                    // already handled in RouteServerImp / run()
                }

                // this elc message was generated after mine was sent
                else if (dateThisElc.after(dateLastSentElc)) {
                    Engine.log("\tNewer ELC. Dropping ...");
                    // drop it
                }

                // this elc message was generated before mine was
                else if (dateThisElc.before(dateLastSentElc)) {
                    Engine.log("\tOlder ELC. Considering and forwarding ...");

                    dateLastSentElc = dateThisElc;
                    requestCountElc = 1;

                    sendMessage(getElcMessage(incomingMessage));
                }

                // this elc is the same as the one initiated by me
                else {
                    Engine.log("\tBINGO! Got back my ELC.");

                    requestCountElc++;

                    switch (requestCountElc) {
                        case 1:
                            // this is the first time you got an elc request: might be leader?
                            Engine.log("\tFirst time. Spreading the word.");
                            sendMessage(getElcMessage(incomingMessage));
                            break;

                        case 2:
                            // this is the second time
                            Engine.log("\tSecond time. Stopping everything. Leader has been elected.");
                            portLeader = getPort(incomingMessage);
                            Engine.log("\t\tLeader: " + portLeader);

                            // TODO show that election is done

                            if (incomingMessage.getOrigin() == Engine.getInstance().serverPort)
                                Engine.log("\tBack at me. I started everything. Stopping.");
                            else
                                sendMessage(getElcMessage(incomingMessage));  // pass on the update
                            break;
                    }
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
    }

    public Route getElcMessage(Route incomingMessage) {
        Route.Builder msg = Route.newBuilder();
        msg.setId(0);
        msg.setOrigin(incomingMessage == null ? Engine.getInstance().serverPort : incomingMessage.getOrigin());
        msg.setDestination(0);
        msg.setPath("");

        int myPriority = getMyPriority();

        synchronized (this) {
            incomingMessage = incomingMessage == null ? this.incomingMessage : incomingMessage;

            if (incomingMessage == null) {
                Engine.log("\tSending ELC before hearing from anyone! Let's go.");
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
                Route.Builder builder = msg.setPayload(
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

//            else {
//
//
//                if (incomingMessage != null) {
//                    String payload1 = new String(incomingMessage.getPayload().toByteArray());
//                    String payload2 = new String(msg.getPayload().toByteArray());
//
//                    Date date1 = Helper.getDate(payload1.split(":")[1]);
//                    Date date2 = Helper.getDate(payload2.split(":")[1]);
//
//                    if (date1.before(date2)) return incomingMessage;
//                }
//            }
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
