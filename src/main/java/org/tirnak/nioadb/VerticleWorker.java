package org.tirnak.nioadb;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

public class VerticleWorker extends AbstractVerticle {

    private Vertx vertx = Vertx.vertx();
    private EventBus eb = vertx.eventBus();

    public void start() {
        System.out.println("verticle worker has started");

    }
}
