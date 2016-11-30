package org.tirnak.nioadb;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.io.IOException;
import java.util.*;


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
        createTable();

        vertx.createHttpServer().requestHandler(r -> {
            jdbc.getConnection(res -> {
                if (res.succeeded()) {
                    SQLConnection connection = res.result();
                    connection.query("SELECT * FROM job", res2 -> {
                        if (res2.succeeded()) {
                            final List<JsonArray> results = res2.result().getResults();
                        }
                        connection.close();
                    });
                } else {
                    // Failed to get connection - deal with it
                }
            });
        }).listen(8080, result -> {
            if (result.succeeded()) {
                fut.complete();
            } else {
                fut.fail(result.cause());
            }
        });

        eb.consumer("device.free", message -> {
            String deviceName = (String) message.body();

            Handler<AsyncResult<ResultSet>> jobHandler = jobQueryResult -> {
                ResultSet resultSet = jobQueryResult.result();
                if (resultSet.getNumRows() == 0) {
                    eb.publish("device.free", deviceName);
                }
                JsonObject jobRow = resultSet.getRows().get(0);
                List<String> instructions = new ArrayList<>(Arrays.asList(jobRow.getString("case").split(";")));
                executeInstructions(instructions, deviceName);

            };

            Handler<AsyncResult<SQLConnection>> getReservedJob = connectionResult -> {
                SQLConnection connection = connectionResult.result();
                String query = "select * from job, case where state = 1 and acquired_by = ? and job.case_id = case.case_id limit 1";
                JsonArray params = new JsonArray().add(0).add(deviceName);
                connection.queryWithParams(query, params, jobHandler);
                connection.close();
            };

            Handler<AsyncResult<SQLConnection>> reserveJob = connectionResult -> {
                SQLConnection connection = connectionResult.result();
                String query = "update job set state = 1 and acquired_by = '" + deviceName + "' where (acc_id, case_id) in " +
                        "(select acc_id, case_id from job where state = 0 limit 1)";
                connection.execute(query, ignored -> {
                    jdbc.getConnection(getReservedJob);
                });
                connection.close();
            };


            jdbc.getConnection(reserveJob);
        });

        System.out.println("verticle mfv has started");

        List<String> devices = Arrays.asList("qwerty", "asdf", "1234");
        devices.forEach(device -> {
            eb.publish("device.free", device);
        });

    }

    private void createTable() {
        jdbc.getConnection(res -> {
            if (res.succeeded()) {

                SQLConnection connection = res.result();

                connection.execute("CREATE TABLE case(case_id INTEGER, case_text CLOB(300))", null)
                    .execute("CREATE TABLE job(acc_id INTEGER, " +
                            "                  case_id INTEGER," +
                            "                  state INTEGER" +
                            "                  acquired_by VARCHAR(30))", null)
                        .execute("INSERT INTO case (case_id, case_text) VALUES (1, 'sleep 1; sleep 2')", null)
                        .execute("INSERT INTO case (case_id, case_text) VALUES (2, 'sleep 1;')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (1,1,0,'')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (1,2,0,'')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (2,1,0,'')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (2,2,0,'')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (3,1,0,'')", null)
                        .execute("INSERT INTO job (acc_id, case_id, state, acquired_by) VALUES (3,2,0,'')", null);
                connection.close();
            } else {}
        });
    }

    private void executeInstructions(List<String> instructions, String deviceName) {
        if (instructions.isEmpty()) { return; }

        vertx.executeBlocking(f -> { try {
                ProcessBuilder pb = new ProcessBuilder(instructions.get(0));
                final Process blockingIO = pb.start();
                blockingIO.waitFor();
            } catch (IOException | InterruptedException ignored) {}},
            r -> {
                instructions.remove(0);
                executeInstructions(instructions, deviceName);
            }
        );
    }
}
