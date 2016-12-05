package org.tirnak.nioadb;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class VerticleWorker extends AbstractVerticle {

    private Vertx vertx = Vertx.vertx();
    private EventBus eb = vertx.eventBus();
    private List<Job> jobs = new CopyOnWriteArrayList<>();

    public void start() {
        System.out.println("verticle worker has started");
        eb.consumer("job", message -> {
            String messageBody = (String) message.body();
            System.out.println(messageBody);
            JsonObject jobParams = new JsonObject(messageBody);
            System.out.println(jobParams);
            ProcessBuilder pb = new ProcessBuilder(jobParams.getString("instruction").split(" "));
            final Process blockingIO;
            try {
                blockingIO = pb.start();
                jobs.add(new Job(blockingIO, jobParams.getString("id")));
            } catch (IOException e) {
                e.printStackTrace();
            }

            vertx.runOnContext(ignored -> {

            });
        });




    }

    private void iterateJobs() {
        boolean needToRepeat = false;
        for (Job job : jobs) {
            if (job.stillRunning()) {
                needToRepeat = true;
            } else {
                eb.publish("job.done", new JobMessage(
                    "", job.id, job.getOutput())
                );
                //TODO write messaging inmplementations
            }
        }
        if (needToRepeat) {
            vertx.runOnContext(ignored -> {
                iterateJobs();
            });
        }
    }



    private class Job {
        private Process workload;
        private String id;

        public Job(Process workload, String id) {
            this.workload = workload;
            this.id = id;
        }

        public boolean stillRunning() {
            return workload.isAlive();
        }

        public String getOutput() {
            check();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(workload.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            try {
                while ( (line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append(System.getProperty("line.separator"));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return builder.toString();
        }

        public int getStatus() {
            check();
            return workload.exitValue();
        }

        private void check() {
            if (workload.isAlive()) {
                throw new  IllegalStateException("reading from alive process is blocking");
            }
        }


    }
}
