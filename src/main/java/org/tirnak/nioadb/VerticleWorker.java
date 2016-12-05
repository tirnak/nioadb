package org.tirnak.nioadb;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.tirnak.nioadb.messaging.JobMessage;
import org.tirnak.nioadb.messaging.JobMessageCodec;
import org.tirnak.nioadb.messaging.JobResultMessage;
import org.tirnak.nioadb.messaging.JobResultMessageCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.tirnak.nioadb.messaging.DeliveryConstants.*;

public class VerticleWorker extends AbstractVerticle {

    private Vertx vertx = Vertx.vertx();
    private EventBus eb = vertx.eventBus();
    private List<Job> jobs = new CopyOnWriteArrayList<>();
    {
        eb.registerDefaultCodec(JobMessage.class, new JobMessageCodec());
        eb.registerDefaultCodec(JobResultMessage.class, new JobResultMessageCodec());
    }

    public void start() {
        System.out.println("verticle worker has started");
        eb.consumer(JOB_MESSAGE_QUEUE_NAME, message -> {
            JobMessage jobMessage = (JobMessage) message.body();
            System.out.println(jobMessage);
            ProcessBuilder pb = new ProcessBuilder(jobMessage.getCommand().split(" "));
            final Process blockingIO;
            try {
                blockingIO = pb.start();
                jobs.add(new Job(blockingIO, jobMessage.getId()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            vertx.runOnContext(ignored -> {
                iterateJobs();
            });
        });

        eb.consumer(JOB_RESULT_MESSAGE_QUEUE_NAME, message -> {
            System.out.println(message.body().toString() + " finished");
        });

        for (String s : new String[]{"sleep 10","sleep 10","sleep 10","sleep 10","sleep 10"}) {
            eb.publish(JOB_MESSAGE_QUEUE_NAME, new JobMessage(s));
        }
    }

    private void iterateJobs() {
        boolean needToRepeat = false;
        synchronized (jobs) {
            for (Job job : jobs) {
                if (job.stillRunning()) {
                    needToRepeat = true;
                } else {
                    eb.publish(JOB_RESULT_MESSAGE_QUEUE_NAME, new JobResultMessage(
                            job.getId(), job.getOutput(), job.getStatus()
                    ));
                    jobs.remove(job);
                }
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

        public String getId() {
            return id;
        }
    }
}
