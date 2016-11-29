package org.tirnak.nioadb.org.tirnak.nioadb.v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;

/**
 * Created by denis on 29.11.16.
 */
public class DeviceManager extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        //Integer id = this.config().getInteger("id");
        Integer id = 1;
        System.out.println("Start worker verticle " + id);
        System.out.println("Config is " + config());
        boolean finish = false;



        EventBus eb = vertx.eventBus();
        eb.consumer("start_" + id, startMessage -> {
            vertx.executeBlocking(future-> {
                while (!finish) { // пока что этот while постоянно шлёт запросы, надо этоп оправить
                    eb.send("getScriptStep", 1, stepMessage -> {
                        String message = (String) stepMessage.result().body();
                        System.out.println("Worker verticle " + id + " execute " + message);
                    });
                }
            }, event -> {

            });

        });
    }
}
