package org.tirnak.nioadb;

import java.util.Random;

public class JobMessage {

    private static Random random = new Random();
    private String command;
    private String id;
    private String stdout;
    private int exitStatus;

    public JobMessage(String command) {
        id = getRandomHexString(10);
        this.command = command;
        this.stdout = "";
    }

    public JobMessage(String command, String id, String result) {
        this.command = command;
        this.id = id;
        this.stdout = result;
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

    public String getStdout() {
        return stdout;
    }
}
