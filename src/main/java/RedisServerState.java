public class RedisServerState {

    private static volatile boolean isLeader = false;
    private static final String LEADER = "role:master";
    private static final String FOLLOWER = "role:slave";

    public static void becomeLeader(){
        isLeader =true;
        System.out.println("This server is the master");
    }

    public static void becomeFollower(){
        isLeader =false;
        System.out.println("This server is the follower");
    }


    public static String getStatus(){
        return isLeader?LEADER:FOLLOWER;
    }

    public static boolean isLeader(){
        return isLeader;
    }

}
