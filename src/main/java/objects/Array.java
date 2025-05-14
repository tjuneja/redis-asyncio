package objects;

import java.util.List;

public class Array implements RedisObject{
    private final List<RedisObject> elements;

    public Array(List<RedisObject> elements){
        this.elements = elements;
    }

    public List<RedisObject> getElements() {
        return elements;
    }

    public boolean isEmpty(){
        return elements.isEmpty();
    }

    public boolean isNull(){
        return elements == null;
    }

    @Override
    public String toString() {
        if(isEmpty() || isNull())
            return "Array{null}";

        StringBuilder sb = new StringBuilder("Array{");

        for(int i = 0; i<elements.size();i++){
            if( i > 0) sb.append(",");

            sb.append(elements.get(i));

        }

        sb.append("}");
        return sb.toString();
    }
}
