import objects.*;
import objects.Error;
import objects.Integer;

import java.nio.charset.StandardCharsets;

public class RedisSerializer {

    public static String serialize(RedisObject redisObject){
        StringBuilder sb = new StringBuilder();
        appendObjects(sb, redisObject);
        return sb.toString();

    }

    private static void appendObjects(StringBuilder sb, RedisObject obj) {
        if(obj instanceof SimpleString){
            SimpleString ss = (SimpleString) obj;
            sb.append('+').append(ss.getValue()).append("\r\n");
        } else if (obj instanceof Error) {
            Error e = (Error) obj;
            sb.append("-").append(e.getValue()).append("\r\n");
        } else if (obj instanceof Integer) {
            Integer i = (Integer) obj;
            sb.append(":").append(i.getValue()).append("\r\n");
        }else if(obj instanceof BulkString){
            BulkString bs = (BulkString) obj;
            if(bs.isNull()){
                sb.append("-1\r\n");
            }else{
                byte[] data = bs.getValue();
                sb.append("$").append(data.length).append("\r\n");
                sb.append(new String(data, StandardCharsets.UTF_8)).append("\r\n");
            }
        }else if(obj instanceof Array){
            Array arr= (Array) obj;

            if(arr.isNull()) sb.append("*-1\r\n");
            else{
                sb.append("*").append(arr.getElements().size()).append("\r\n");
                for(RedisObject redisObject: arr.getElements()) {
                    appendObjects(sb, redisObject);
                }

            }
        }
    }




}
