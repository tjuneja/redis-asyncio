import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisStore {
    private static Map<String,byte[]> dataStore = new ConcurrentHashMap<>();
    private static Map<String,Long> expirationTimes = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    static {
        scheduler.scheduleAtFixedRate(RedisStore::expireKeys,100, 100, TimeUnit.MILLISECONDS);
    }


    public static void set(String key, byte[] value){
        set(key, value,-1);
    }

    public static void set(String key, byte[] value, long expiresAt){
        dataStore.put(key, value);

        if(expiresAt > 0) expirationTimes.put(key, expiresAt);
        else expirationTimes.remove(key);
    }


    public static byte[] get(String key){
        if(!dataStore.containsKey(key)) return null;

        if(isExpired(key)){
            delete(key);
            return null;
        }

        return dataStore.get(key);
    }


    private static boolean isExpired(String key){
        Long expiry = expirationTimes.get(key);

        if(expiry == null) return false;

        return System.currentTimeMillis() > expiry;


    }

    private static void delete(String key){
        dataStore.remove(key);
        expirationTimes.remove(key);
    }

    private static void expireKeys() {
        Set<String> deleteSet = new HashSet<>();

        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry: expirationTimes.entrySet()){
            if(entry.getValue() <= now){
                deleteSet.add(entry.getKey());
            }
        }
        deleteSet.forEach(RedisStore::delete);
    }


}
