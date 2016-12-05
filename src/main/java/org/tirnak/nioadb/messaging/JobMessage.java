package org.tirnak.nioadb.messaging;

import java.util.Random;

public class JobMessage {

    private static Random random = new Random();
    private String command;
    private String id;

    public JobMessage(String command) {
        id = getRandomHexString(10);
        this.command = command;
    }

    public JobMessage(String command, String id) {
        this.command = command;
        this.id = id;
    }

    private String getRandomHexString(int numchars) {
        StringBuffer sb = new StringBuffer();
        while(sb.length() < numchars){
            sb.append(Integer.toHexString(random.nextInt()));
        }

        return sb.toString().substring(0, numchars);
    }

    public String getCommand() {
        return command;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "JobMessage{" +
                "command='" + command + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
