package objects;

public class SimpleString implements RedisObject{
    private final String value;

    public SimpleString(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "SimpleString{" + value +  '}';
    }
}
