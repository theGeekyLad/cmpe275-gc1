package org.thegeekylad.util;

public class MyLogger {
    public String logLabel, consoleColor;

    public MyLogger(String logLabel, String consoleColor) {
        this.logLabel = logLabel;
        this.consoleColor = consoleColor == null ? ConsoleColors.RESET : consoleColor;
    }

    public void logDivider() {
        log("---------------------------------------------------");
    }

    public void log(String message) {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        StackTraceElement stackTraceElement = stackTraceElements[stackTraceElements.length - 1 - 1];

//        System.out.print(consoleColor + String.format("[%s] [%s] ", stackTraceElement.getClassName(), stackTraceElement.getMethodName()));
        System.out.print(consoleColor + String.format("[%s] ", stackTraceElement.getMethodName()));
        System.out.println(consoleColor + message);
//        System.out.println((logLabel == null ? "" : String.format("[%s] ", logLabel)) + message);
    }
}
