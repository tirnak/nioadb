package org.tirnak.nioadb.org.tirnak.nioadb.v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * Created by denis on 29.11.16.
 */
public class Scheduler extends AbstractVerticle {

    public void start(Future<Void> fut) {

        EventBus eb = vertx.eventBus();
        vertx.deployVerticle("org.tirnak.nioadb.org.tirnak.nioadb.v2.ScriptStorage");

        DeploymentOptions workerOptions = new DeploymentOptions()
                .setWorker(true)
                .setConfig(new JsonObject());

        workerOptions.getConfig().put("id", 1);
        vertx.deployVerticle("org.tirnak.nioadb.org.tirnak.nioadb.v2.DeviceManager", new DeploymentOptions().setWorker(true));
        /*
        workerOptions.getConfig().put("id", 2);
        vertx.deployVerticle("org.tirnak.nioadb.org.tirnak.nioadb.v2.DeviceManager", workerOptions);
        */

        long timerID = vertx.setTimer(1000, id -> {
            eb.send("start_1", "");
        });

        System.out.println("Hello");
    }
}
