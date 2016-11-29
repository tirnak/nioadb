package org.tirnak.nioadb;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class MyFirstVerticle extends AbstractVerticle {

    private Vertx vertx = Vertx.vertx();
    private EventBus eb = vertx.eventBus();
    private Queue<String> flowBuffer = new LinkedList<>();
    private static final int MAX_SIZE = 10;

    private JDBCClient jdbc;
    {
        JsonObject config = new JsonObject().put("url", "jdbc:hsqldb:mem:test?shutdown=true")
                .put("driver_class", "org.hsqldb.jdbcDriver");
        jdbc = JDBCClient.createNonShared(vertx, config);
    }

    public void start(Future<Void> fut) {

        // Connect to the database
        jdbc.getConnection(res -> {
            if (res.succeeded()) {

                SQLConnection connection = res.result();

                connection.execute("CREATE TABLE test(col VARCHAR(20))", null)
                        .execute("INSERT INTO test (col) VALUES ('val1')", null)
                        .execute("INSERT INTO test (col) VALUES ('val2')", null);
                connection.close();
            } else {}
        });

//        DeploymentOptions options = new DeploymentOptions().setWorker(true);
//        vertx.deployVerticle("org.tirnak.nioadb.VerticleWorker", options);

        vertx.executeBlocking(f -> {
            int x = 1;
            while (true) {
                System.out.printf("sending " + x);
                eb.publish("flow", "x is " + x++);
            }},
        null);

        eb.consumer("flow", message -> {
            System.out.println(message);
            if (flowBuffer.size() == MAX_SIZE) {
                flowBuffer.poll();
            }
            flowBuffer.add(message.body().toString());
        });

        vertx
        .createHttpServer()
        .requestHandler(r -> {
            jdbc.getConnection(res -> {
                if (res.succeeded()) {
                    SQLConnection connection = res.result();
                    connection.query("SELECT * FROM test", res2 -> {
                        if (res2.succeeded()) {
                            final List<JsonArray> results = res2.result().getResults();
                        }
                        connection.close();
                    });
                } else {
                    // Failed to get connection - deal with it
                }
            });
        })
        .listen(8080, result -> {
            if (result.succeeded()) {
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });

        System.out.println("verticle mfv has started");

    }
}
