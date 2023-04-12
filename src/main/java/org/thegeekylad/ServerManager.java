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

public class ServerManager {
    private Route incomingMessage;
    private Integer portLeader;
    volatile private Date dateLastSentElc;
    private RouteServerImpl routeServer;

    // ----------------------------------------------------

    public long requestCount;
    public long requestCountElc;
    public ServerManager() {
        requestCount = 0;
        requestCountElc = 0;
        portLeader = null;

        Engine.log("Service manager created.");
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

    private void setLeaderPort(Route incomingMessage) {
        String payload = new String(incomingMessage.getPayload().toByteArray());
        portLeader = Integer.parseInt(payload.split("#")[3]);
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

    // ----------------------------------------------------

    public void sendMessage(Route msg) {
        while (true) {
            try {
                Link link = Engine.getInstance().links.get(0);
                RouteClient.run(link.getPort(), msg);
                Engine.log("Connected. Sent.");

                break;
            } catch (Exception e) {
//                e.printStackTrace();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                }

                Engine.log("Couldn't connect, retrying ...");
            }
        }
    }

    public int getMyPriority() {
        return 1;  // TODO something meaningful
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

                    // this is the second time
                    if (++requestCountElc == 2) {
                        Engine.log("\tSecond time. Stopping everything. Leader has been elected.");

                        setLeaderPort(incomingMessage);
                        // TODO show that election is done
                        return;  // stop election
                    }

                    // this is the first time you got an elc request: might be leader?
                    Engine.log("\tFirst time. Spreading the word.");
                    sendMessage(getElcMessage(incomingMessage));
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
        msg.setOrigin(0);
        msg.setDestination(0);
        msg.setPath("");

        synchronized (this) {
            incomingMessage = incomingMessage == null ? this.incomingMessage : incomingMessage;

            int myPriority = getMyPriority();
            int priority = incomingMessage == null ? myPriority : Math.max(myPriority, getPriority(incomingMessage));
            String timestamp = incomingMessage == null
                    ? Helper.getTimestampString(dateLastSentElc = new Date())
                    : Helper.getTimestampString(getTimestamp(incomingMessage));

            if (incomingMessage == null) Engine.log("\tSending ELC before hearing from anyone! Let's go.");
            if (incomingMessage == null) Engine.log(dateLastSentElc == null ? "\tNull :/" : "\tDate present!");

            msg.setPayload(
                    ByteString.copyFrom(
                            new StringBuilder()
                                    .append(MessageType.ELC.name())
                                    .append("#")
                                    .append(timestamp)
                                    .append("#")
                                    .append(priority)
                                    .toString()
                                    .getBytes()
                    )
            );

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
