package org.thegeekylad.server;

public class Lock {
    public static final Object lockIncomingMessage = new Object();
    public static final Object lockHbtMessage = new Object();
}
