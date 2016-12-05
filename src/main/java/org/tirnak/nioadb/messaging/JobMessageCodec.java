package org.tirnak.nioadb.messaging;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class JobMessageCodec implements MessageCodec<JobMessage, JobMessage>{

    @Override
    public void encodeToWire(Buffer buffer, JobMessage jobMessage) {
        JsonObject jsonToEncode = new JsonObject();
        jsonToEncode.put("command", jobMessage.getCommand());
        jsonToEncode.put("id", jobMessage.getId());

        // Encode object to string
        String jsonToStr = jsonToEncode.encode();

        // Length of JSON: is NOT characters count
        int length = jsonToStr.getBytes().length;

        // Write data into given buffer
        buffer.appendInt(length);
        buffer.appendString(jsonToStr);
    }

    @Override
    public JobMessage decodeFromWire(int position, Buffer buffer) {
        // My custom message starting from this *position* of buffer
        int _pos = position;

        // Length of JSON
        int length = buffer.getInt(_pos);

        // Get JSON string by it`s length
        // Jump 4 because getInt() == 4 bytes
        String jsonStr = buffer.getString(_pos+=4, _pos+=length);
        JsonObject contentJson = new JsonObject(jsonStr);

        // Get fields
        String command = contentJson.getString("command");
        String id = contentJson.getString("id");

        // We can finally create custom message object
        return new JobMessage(command, id);
    }

    @Override
    public JobMessage transform(JobMessage customMessage) {
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
