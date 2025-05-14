package objects;

public class Error implements RedisObject{

    private final String value;

    public Error(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Error{" + value +  '}';
    }
}
