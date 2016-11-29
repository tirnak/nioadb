package org.tirnak.nioadb.org.tirnak.nioadb.v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;

import java.util.HashMap;

/**
 * Created by denis on 29.11.16.
 */
public class ScriptStorage extends AbstractVerticle {

    public void start(Future<Void> fut) {

        System.out.println("ScriptStorage started");

        HashMap<Integer, String> script1 = new HashMap<>();
        script1.put(1, "Command 1");
        script1.put(2, "Command 1");
        script1.put(3, "END");


        EventBus eb = vertx.eventBus();
        eb.consumer("getScriptStep", message -> {
            Integer stepNumber = (Integer)message.body();
            System.out.println("return step " + stepNumber );
            String stepContent = script1.get(stepNumber);
            message.reply(stepContent);
        });

    }
}
