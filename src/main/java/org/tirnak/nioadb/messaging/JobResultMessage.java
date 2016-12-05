package org.tirnak.nioadb.messaging;

import java.util.Random;

public class JobResultMessage {

    private String id;
    private String stdout;
    private int exitStatus;

    public JobResultMessage(String id, String stdout, int exitStatus) {
        this.id = id;
        this.stdout = stdout;
        this.exitStatus = exitStatus;
    }

    public String getId() {
        return id;
    }

    public String getStdout() {
        return stdout;
    }

    public int getExitStatus() {
        return exitStatus;
    }

    @Override
    public String toString() {
        return "JobResultMessage{" +
                "id='" + id + '\'' +
                ", stdout='" + stdout + '\'' +
                ", exitStatus=" + exitStatus +
                '}';
    }
}
