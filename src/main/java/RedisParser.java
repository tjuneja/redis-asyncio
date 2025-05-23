import objects.Array;
import objects.BulkString;
import objects.Error;
import objects.Integer;
import objects.RedisObject;
import objects.SimpleString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RedisParser {

   public static RedisObject parse(String input) throws IOException {
       return parse(input.getBytes(StandardCharsets.UTF_8));
   }

    private static RedisObject parse(byte[] bytes) throws IOException {
        try(ByteArrayInputStream input = new ByteArrayInputStream(bytes)){
            return parse(input);
        }
   }

    private static RedisObject parse(InputStream input) throws IOException {
        int firstByte = input.read();
        if(firstByte == -1){
            throw new IOException("Unexpected end of stream");
        }

        switch (firstByte){
            //SimpleString
            case '+':
                return new SimpleString(readLine(input));
            //Array
            case '*':
                return parseArray(input);
            case '$':
                return parseBulkString(input);
            case ':':
                String line = readLine(input);
                try {
                    return new Integer(Long.parseLong(line));
                }catch (Exception e){
                    throw new IOException("Invalid integer");
                }
            case '-':
                return new Error(readLine(input));
            default:
                throw new IOException("Invalid RESP data type" + (char)firstByte);
        }


    }

    private static RedisObject parseBulkString(InputStream input) throws IOException {
        String length = readLine(input);
        int len ;
        try{
            len = java.lang.Integer.parseInt(length);
        } catch (Exception e) {
            throw new IOException("Illegal characters",e);
        }

        // A length of -1 indicates a null bulk string in RESP
        if (len == -1) {
            return new BulkString(null);
        }

        if (len < 0) {
            throw new IOException("Invalid bulk string length: " + length);
        }
        int bytesRead = 0;
        byte[] value = new byte[len];
        while(bytesRead < len){
            int count = input.read(value, bytesRead, len- bytesRead);
            if (count == -1) throw new IOException("Unexpected end of stream while reading bulk strings");
            bytesRead+=count;
        }

        int cr = input.read();
        int lf =input.read();

//        if(cr !='\r' || lf !='\n')
//            throw new IOException("Expected CRLF after bulk string, got: " + (char) cr + (char) lf);

        return new BulkString(value);

    }

    private static RedisObject parseArray(InputStream input) throws IOException {
       String length = readLine(input);
       int len;

       try{
           len = java.lang.Integer.parseInt(length);
       } catch (NumberFormatException e) {
           throw new IOException("Invalid array length: " + length, e);
       }

       if(len == -1){
           return  new Array(null);
       }

        List<RedisObject> objects = new ArrayList<>(len);

        for(int i = 0; i< len;i++){
            objects.add(parse(input));
        }

        return new Array(objects);
    }

    private static String readLine(InputStream input) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while( (b = input.read()) != -1){
            if(b == '\r'){
                int next = input.read();

                if(next != '\n'){
                    throw new IOException("Illegal character");
                }
                break;
            }
            sb.append((char) b);
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        try {
            // Parse a PING command
            String pingCommand = "*1\r\n$4\r\nPING\r\n";
            Array obj = (Array) parse(pingCommand);
            System.out.println("Parsed: " + obj.getElements());
            String setCommand = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n:100\r\n";
            String getCommand = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";

            // Parse a more complex command like SET key value
            //String setCommand = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
            System.out.println("Parsed: " + parse(getCommand));
//
//            // Parse an integer response
//            String intResponse = ":1000\r\n";
//            System.out.println("Parsed: " + parse(intResponse));
////
////            // Parse an error response
//            String errorResponse = "-ERR unknown command 'foobar'\r\n";
//            System.out.println("Parsed: " + parse(errorResponse));
//
////            // Parse a null bulk string
//            String nullBulkString = "$-1\r\n";
//            System.out.println("Parsed: " + parse(nullBulkString));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
