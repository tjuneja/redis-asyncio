package objects;

import java.nio.charset.StandardCharsets;

public class BulkString implements RedisObject{
    private final byte[] value;


    public BulkString(byte[] value){
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public boolean isNull(){
        return value == null;
    }

    public String getValueAsString(){
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "BulkString{"  + (value == null ? "" : getValueAsString()) + '}';
    }
}
