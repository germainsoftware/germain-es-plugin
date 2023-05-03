package com.germainsoftware.elasticsearch;

public class GermainLogger {

    // Debug logger
    public static void log(String msg) {
        System.err.println(msg);
    }
    
    public static void logStack(String msg) {
        final var sb = new StringBuilder();
        sb.append(msg);
        sb.append("\n");
        final var stack = Thread.currentThread().getStackTrace();
        for (var i = 0; i < stack.length; i++) {
            sb.append(stack[i].getClassName());
            sb.append(".");
            sb.append(stack[i].getMethodName());
            sb.append("(");
            sb.append(stack[i].getFileName());
            sb.append(":");
            sb.append(stack[i].getLineNumber());
            sb.append(")\n");
        }
        System.err.println(sb.toString());
    }    
}
