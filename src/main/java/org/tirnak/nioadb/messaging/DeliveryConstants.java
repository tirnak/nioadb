package org.tirnak.nioadb.messaging;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;

public class DeliveryConstants {
//    public final static DeliveryOptions JOB_MESSAGE_OPTIONS;
//    public final static DeliveryOptions JOB_RESULT_MESSAGE_OPTIONS;
    public final static String JOB_MESSAGE_QUEUE_NAME = "job.queue";
    public final static String JOB_RESULT_MESSAGE_QUEUE_NAME = "job.done";
    public static final String QUEUE_DEVICE_FREE_NAME = "device.free";

    static {
//        JobMessageCodec jobMessageCodec = new JobMessageCodec();
//        JobResultMessageCodec jobResultMessageCodec = new JobResultMessageCodec();
//        JOB_MESSAGE_OPTIONS = new DeliveryOptions().setCodecName(jobMessageCodec.name());
//        JOB_RESULT_MESSAGE_OPTIONS = new DeliveryOptions().setCodecName(jobResultMessageCodec.name());
        EventBus eb = Vertx.vertx().eventBus();
        eb.registerDefaultCodec(JobMessage.class, new JobMessageCodec());
        eb.registerDefaultCodec(JobResultMessage.class, new JobResultMessageCodec());
    }
}
