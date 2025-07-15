package v2;

import java.nio.channels.SocketChannel;

public class ConnectionBuffer{
    private final StringBuilder accumulator = new StringBuilder();
    private final SocketChannel channel;
    private volatile boolean closed;


    public ConnectionBuffer(SocketChannel channel){
        this.channel = channel;
    }

    public synchronized void appendData(String data){
        if(closed) {
            throw new IllegalArgumentException("Cannot append to closed buffer");
        }
        this.accumulator.append(data);
    }

    public synchronized void appendData(byte[] data){
        appendData(new String(data));
    }

    public SocketChannel getChannel(){
        return this.channel;
    }

    public synchronized String getAccumulatedData(){
        return this.accumulator.toString();
    }

    public void consumeData(int charactersConsumed){

        if (charactersConsumed < 0) {
            throw new IllegalArgumentException("Cannot consume negative characters");
        }

        if(charactersConsumed >= accumulator.length()){
            this.accumulator.setLength(0); // clear all
        }else{
            String remaining = this.accumulator.substring(charactersConsumed);
            accumulator.setLength(0);
            accumulator.append(remaining);
        }
    }
    public synchronized void clear() {
        accumulator.setLength(0);
    }


    /**
     * Get the length of buffered data
     */
    public synchronized int getDataLength() {
        return accumulator.length();
    }
    /**
     * Check if buffer is closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Close the buffer (prevents further operations)
     */
    public synchronized void close() {
        closed = true;
        accumulator.setLength(0);
    }


    public boolean hasData() {
        return !accumulator.isEmpty();
    }
}