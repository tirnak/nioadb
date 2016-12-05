package org.tirnak.nioadb.messaging;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class JobResultMessageCodec implements MessageCodec<JobResultMessage, JobResultMessage>{

    @Override
    public void encodeToWire(Buffer buffer, JobResultMessage jobResultMessage) {
        JsonObject jsonToEncode = new JsonObject();
        jsonToEncode.put("id", jobResultMessage.getId());
        jsonToEncode.put("stdout", jobResultMessage.getStdout());
        jsonToEncode.put("exitStatus", jobResultMessage.getExitStatus());

        // Encode object to string
        String jsonToStr = jsonToEncode.encode();

        // Length of JSON: is NOT characters count
        int length = jsonToStr.getBytes().length;

        // Write data into given buffer
        buffer.appendInt(length);
        buffer.appendString(jsonToStr);
    }

    @Override
    public JobResultMessage decodeFromWire(int position, Buffer buffer) {
        // My custom message starting from this *position* of buffer
        int _pos = position;

        // Length of JSON
        int length = buffer.getInt(_pos);

        // Get JSON string by it`s length
        // Jump 4 because getInt() == 4 bytes
        String jsonStr = buffer.getString(_pos+=4, _pos+=length);
        JsonObject contentJson = new JsonObject(jsonStr);

        // Get fields
        String id = contentJson.getString("id");
        String stdout = contentJson.getString("stdout");
        int exitStatus = contentJson.getInteger("exitStatus");

        // We can finally create custom message object
        return new JobResultMessage(id, stdout, exitStatus);
    }

    @Override
    public JobResultMessage transform(JobResultMessage customMessage) {
        // If a message is sent *locally* across the event bus.
        // This example sends message just as is
        return customMessage;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        // Always -1
        return -1;
    }
}
