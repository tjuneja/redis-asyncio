package connection;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionManager {

    public enum ConnectionType{
        CLIENT,
        MASTER,
        REPLICA
    }


    public static class ConnectionInfo{
        private final SocketChannel channel;
        private final ConnectionType connectionType;
        private final long connectionTime;


        public ConnectionInfo(SocketChannel channel, ConnectionType connectionType){
            this.channel = channel;
            this.connectionType = connectionType;
            this.connectionTime = System.currentTimeMillis();
        }

        public SocketChannel getChannel() {
            return channel;
        }

        public long getConnectionTime() {
            return connectionTime;
        }

        public ConnectionType getConnectionType() {
            return connectionType;
        }
    }


    private static final List<SocketChannel> replicaConnections = new CopyOnWriteArrayList<>();
    private static final Map<SocketChannel, ConnectionInfo> connections = new ConcurrentHashMap<>();
    private static volatile SocketChannel masterConnection = null;

    public static void registerClientConnection(SocketChannel channel){
        connections.put(channel, new ConnectionInfo(channel, ConnectionType.CLIENT));
        System.out.println("Register client connection : "+channel);
    }

    public static void registerReplicaConnection(SocketChannel channel){
        ConnectionInfo connectionInfo = new ConnectionInfo(channel, ConnectionType.REPLICA);
        connections.put(channel, connectionInfo);
        replicaConnections.add(channel);
        System.out.println("Registered replica : "+ channel);
    }

    public static void setMasterConnection(SocketChannel channel){
        masterConnection = channel;
        connections.put(channel, new ConnectionInfo(channel, ConnectionType.MASTER));
        System.out.println("Registered master : "+ channel);
    }

    public static void removeConnection(SocketChannel channel){
        ConnectionInfo connectionInfo = connections.remove(channel);
        if(connectionInfo != null){
            if(connectionInfo.connectionType == ConnectionType.MASTER){
                masterConnection = null;
                System.out.println("Removing master connection");
            } else if (connectionInfo.connectionType == ConnectionType.REPLICA) {
                replicaConnections.remove(channel);
            }
        }
    }

    public static ConnectionType getConnectionType(SocketChannel channel){
        ConnectionInfo info = connections.get(channel);
        return info != null ? info.connectionType : ConnectionType.CLIENT;
    }

    public static SocketChannel getMasterConnection(){
        return masterConnection;
    }

    public static List<SocketChannel> getReplicaConnections(){
        return new CopyOnWriteArrayList<>(replicaConnections);
    }

    public static boolean hasReplicas(){
        return !replicaConnections.isEmpty();
    }



}
