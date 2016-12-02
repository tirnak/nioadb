package org.tirnak.nioadb;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;


public class MyFirstVerticle extends AbstractVerticle {

    private Vertx vertx = Vertx.vertx();
    private EventBus eb = vertx.eventBus();
    private Random random = new Random();
    private JDBCClient jdbc;
    {
        JsonObject config = new JsonObject().put("url", "jdbc:hsqldb:mem:test?shutdown=true")
                .put("driver_class", "org.hsqldb.jdbcDriver");
        jdbc = JDBCClient.createNonShared(vertx, config);
    }

    public void start(Future<Void> fut) throws IOException, InterruptedException {

        vertx.createHttpServer().requestHandler(r -> {
            jdbc.getConnection(res -> {
                if (res.succeeded()) {
                    SQLConnection connection = res.result();
                    connection.query("SELECT * FROM job", res2 -> {
                        List<JsonArray> results = res2.result().getResults();
                        r.response().end(results.toString());
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
                    return;
                }
                JsonObject jobRow = resultSet.getRows().get(0);
                JsonObject params = new JsonObject().put("ACC_ID", jobRow.getValue("ACC_ID"))
                                                    .put("SCENARIO_ID", jobRow.getValue("SCENARIO_ID"));
                ArrayList<String> instructions = new ArrayList<>(Arrays.asList(jobRow.getString("SCENARIO_TEXT").split(";")));
                executeInstructions(instructions, deviceName, params);
            };

            Handler<AsyncResult<SQLConnection>> getReservedJob = connectionResult -> {
                SQLConnection connection = connectionResult.result();
                String query = "select * from job, scenario where state = 1 and acquired_by = ? and job.scenario_id = scenario.scenario_id limit 1";
                JsonArray params = new JsonArray().add(deviceName);
                connection.queryWithParams(query, params, jobHandler);
                connection.close();
            };

            Handler<AsyncResult<SQLConnection>> reserveJob = connectionResult -> {
                SQLConnection connection = connectionResult.result();
                String query = "update job set state = 1, acquired_by = '" + deviceName + "' where  " +
                        "state = 0 and ROWNUM() <= 1";
                connection.update(query, ignored -> jdbc.getConnection(getReservedJob));
                connection.close();
            };

            jdbc.getConnection(reserveJob);
        });

        System.out.println("verticle mfv has started");

        Handler<AsyncResult<List<Integer>>> initialSendDevices = ignored -> {
            List<String> devices = Arrays.asList("qwerty", "asdf", "1234");
            devices.forEach(device -> {
                eb.publish("device.free", device);
            });
        };

        Handler<AsyncResult<SQLConnection>> createSampleData = connectionResult -> {
            SQLConnection connection = connectionResult.result();

            List<String> batch = new ArrayList<>();
            batch.add("CREATE TABLE IF NOT EXISTS scenario(scenario_id INTEGER, scenario_text CLOB(300))");
            batch.add("CREATE TABLE IF NOT EXISTS job(acc_id INTEGER, " +
                    "                  scenario_id INTEGER," +
                    "                  state INTEGER," +
                    "                  acquired_by VARCHAR(30))");
            batch.add("INSERT INTO scenario (scenario_id, scenario_text) VALUES (1, 'sleep 1;sleep 2')");
            batch.add("INSERT INTO scenario (scenario_id, scenario_text) VALUES (2, 'sleep 1;')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (1,1,0,'')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (1,2,0,'')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (2,1,0,'')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (2,2,0,'')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (3,1,0,'')");
            batch.add("INSERT INTO job (acc_id, scenario_id, state, acquired_by) VALUES (3,2,0,'')");
            connection.batch(batch, initialSendDevices);
            connection.close();
        };

        jdbc.getConnection(createSampleData);

    }

    private void executeInstructions(ArrayList<String> instructions, String deviceName, JsonObject params) {

        Handler<AsyncResult<SQLConnection>> completeJob = connectionResult -> {
            SQLConnection connection = connectionResult.result();
            String query = "update job set state = 2 where " +
                    "state = 1 and ACC_ID = ? and SCENARIO_ID = ?";
            JsonArray completeParams = new JsonArray().add(params.getValue("ACC_ID")).add(params.getValue("SCENARIO_ID"));
            connection.updateWithParams(query, completeParams,
                ignored -> eb.publish("device.free", deviceName)
            );
            connection.close();
        };

        if (instructions.isEmpty()) {
            jdbc.getConnection(completeJob);
        }

        Handler<Future<Object>> instructionHandler = f -> {
            Logger logger = new DeviceLogger(random.nextInt(), deviceName, System.out);
            try {
                logger.log("start to execute: ");
                for (String s : instructions.get(0).split(" ")) {
                    logger.log(s);
                }
                ProcessBuilder pb = new ProcessBuilder(instructions.get(0).split(" "));
                final Process blockingIO = pb.start();
                blockingIO.waitFor();
                logger.log("executed successfully ");
            } catch (IOException | InterruptedException e) {
                logger.log("exception at ");
                e.printStackTrace();
            } finally {
                instructions.remove(0);
                if (instructions.isEmpty()) {
                    jdbc.getConnection(completeJob);
                }
                f.complete(instructions);
            }
        };

        Handler<AsyncResult<Object>> schedulingHandler = instructionResult -> {
            executeInstructions((ArrayList<String>) instructionResult.result(), deviceName, params);
        };

        vertx.executeBlocking(instructionHandler, schedulingHandler);
    }

    private class Logger {
        protected int id;
        protected PrintStream ps;
        public Logger(int id, PrintStream ps) {
            this.id = id;
            this.ps = ps;
        }
        public void log(String s) {
            ps.println(id + ": " + s);
        }
    }

    private class DeviceLogger extends Logger {
        String deviceName;
        public DeviceLogger(int id, String deviceName, PrintStream ps) {
            super(id, ps);
            this.deviceName = deviceName;
        }
        public void log(String s) {
            ps.println(id + ": " + s);
        }
    }
}
