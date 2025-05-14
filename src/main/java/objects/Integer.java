package objects;

public class Integer implements RedisObject{
    private final long value;

    public Integer(long value){
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Integer{" + value +  '}';
    }


}
