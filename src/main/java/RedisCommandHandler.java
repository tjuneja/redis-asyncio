import objects.Array;
import objects.BulkString;
import objects.RedisObject;
import objects.SimpleString;

import java.io.IOException;
import java.util.List;

public class RedisCommandHandler {

    public static RedisObject executeCommand(RedisObject parsedCommand) throws IOException {
        if(!(parsedCommand instanceof Array commands)) throw new IOException("Command should be an array");

        List<RedisObject> redisObjects = commands.getElements();

        if(redisObjects == null || redisObjects.isEmpty()) throw new IOException("Empty command");

        if(!(redisObjects.get(0) instanceof BulkString commandName)) throw new IOException("First input should be a command");

        String command = commandName.getValueAsString().toUpperCase();

        switch (command){
            case "PING":
                return new SimpleString("PONG");
            case "ECHO":
                if(redisObjects.size() < 2)
                    throw new IOException("ECHO requires an argument");
                return redisObjects.get(1);
            case "SET":
                handleSet(redisObjects);
            case "GET":
                handleGet(redisObjects);

            default:
                throw new IOException("Unsupported command");
        }

    }

    private static RedisObject handleGet(List<RedisObject> redisObjects) throws IOException {
//        if(redisObjects.size() != 2) throw new IOException(" Wrong number of arguments");

        if(!(redisObjects.get(1) instanceof BulkString)){
            throw new IOException("Key should be a string");
        }
        BulkString key = (BulkString) redisObjects.get(1);
        byte[] value = RedisStore.get(key.getValueAsString());
        if(value == null) return new BulkString(null);

        return new BulkString(value);

    }

    private static void handleSet(List<RedisObject> redisObjects) throws IOException {
        if (redisObjects.size() <3)
            throw new IOException("Wrong number of arguments for set command");

        if(!(redisObjects.get(1) instanceof BulkString) || !(redisObjects.get(2) instanceof BulkString)){
            throw new IOException("ERR key and value must be strings");
        }

        String key = ((BulkString) redisObjects.get(1)).getValueAsString();
        byte[] value = ((BulkString) redisObjects.get(2)).getValue();

        RedisStore.set(key, value);
    }


}
