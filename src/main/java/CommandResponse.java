import objects.RedisObject;

import java.util.ArrayList;
import java.util.List;

public class CommandResponse {
    private final List<ResponsePart> parts;
    private boolean isComplete;


    public CommandResponse(){
        this.parts = new ArrayList<>();
        this.isComplete = true;
    }

    public CommandResponse(String singleResponse){
        this();
        addStringObject(singleResponse);
    }

    public CommandResponse addStringObject(String response){
        this.parts.add(new ResponsePart(response.getBytes(), ResponsePart.Type.RESP));
        return this;
    }

    public CommandResponse addRespObject(RedisObject redisObject){
        String response = RedisSerializer.serialize(redisObject);
        addStringObject(response);
        return this;
    }

    public CommandResponse addBinaryObject(byte[] data){
        this.parts.add(new ResponsePart(data, ResponsePart.Type.BINARY));
        return this;
    }
    public CommandResponse setMultiPart(){
        this.isComplete = false;
        return this;
    }

    public boolean hasResponse(){
        return !this.parts.isEmpty();
    }

    public boolean hasResponses(){
        return this.parts.size() > 1;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public boolean isMultiPart(){
        return this.parts.size() > 1;
    }
    public String getStringResponse(){
        if(this.parts.isEmpty()) return null;

        return new String(this.parts.getFirst().getData());
    }

    public List<ResponsePart> getParts() {
        return this.parts;
    }

    public static class ResponsePart {

        private final byte[] data;
        private final Type type;
        public ResponsePart(byte[] data, Type type) {
            this.data = data;
            this.type = type;
        }

        public enum Type{
            RESP,
            BINARY
        }


        public Type getType() {
            return type;
        }

        public byte[] getData() {
            return data;
        }

        public boolean isResp(){
            return this.type == Type.RESP;
        }

        public boolean isBinary(){
            return this.type == Type.BINARY;
        }
    }
}
