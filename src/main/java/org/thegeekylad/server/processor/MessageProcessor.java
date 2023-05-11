package org.thegeekylad.server.processor;

import com.google.protobuf.ByteString;
import gash.grpc.route.server.Engine;
import org.thegeekylad.util.constants.QueryType;
import route.Route;

public class MessageProcessor {
    public enum Type {
        HBT, ELC, QRY, RES, NEW, RNG;
    }

    public static String getPayload(Route msg) {
        return new String(msg.getPayload().toByteArray());
    }

    public static String getType(Route msg) {
        return getPayload(msg).split("#")[0];
    }

    // ----------------------------------------------------
    // elc message management
    // ----------------------------------------------------
    public static class Elc {
        public static int getCount(Route msg) {
            String payload = getPayload(msg);
            return Integer.parseInt(payload.split("#")[4]);
        }
    }

    // ----------------------------------------------------
    // hbt message management
    // ----------------------------------------------------
    public static class Hbt {
        public static Route getMessage(Integer portDestination) {
            return Route.newBuilder()
                    .setId(0)
                    .setOrigin(Engine.getInstance().serverPort)
                    .setDestination(portDestination)
                    .setPath("")
                    .setPayload(ByteString.copyFrom(MessageProcessor.Type.HBT.name().getBytes()))
                    .build();
        }
    }

    // ----------------------------------------------------
    // qry message management
    // ----------------------------------------------------
    public static class Qry {
        public static Route getMessage(String id, QueryType queryType, String data, Integer portDestination) {
            String payload = MessageProcessor.Type.QRY.name() + "#" + id + "#" + queryType.name() + "#" + data;
            return Route.newBuilder()
                    .setId(0)
                    .setOrigin(Engine.getInstance().serverPort)
                    .setDestination(portDestination)
                    .setPath("")
                    .setPayload(ByteString.copyFrom(payload.getBytes()))
                    .build();
        }

        public static String getId(Route msg) {
            String payload = MessageProcessor.getPayload(msg);
            return payload.split("#")[1];
        }

        public static String getData(Route msg) {
            String payload = MessageProcessor.getPayload(msg);
            return payload.split("#")[3];
        }

        public static String getType(Route msg) {
            String payload = getPayload(msg);
            return payload.split("#")[2];
        }
    }

    // ----------------------------------------------------
    // res message management
    // ----------------------------------------------------
    public static class Res {
        public static String getQueryId(Route msg) {
            String payload = getPayload(msg);
            return payload.split("#")[1];
        }

        public static String getData(Route msg) {
            String payload = getPayload(msg);
            return payload.split("#")[2];
        }

        public static Route getMessage(String queryId, String data) {
            String payload = Type.RES.name() + "#" + queryId + "#" + data;
            return Route.newBuilder()
                    .setId(0)
                    .setOrigin(Engine.getInstance().serverPort)
                    .setDestination(0)
                    .setPath("")
                    .setPayload(ByteString.copyFrom(payload.getBytes()))
                    .build();
        }
    }
}
