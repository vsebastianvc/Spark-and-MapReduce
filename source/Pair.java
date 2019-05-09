package eRFactor;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Pair implements Writable {

	
    private Double key;
    private Long value;
    
    

    public Pair() {
    	
	}

	public Pair(Double key, Long value) {
        this.key = key;
        this.value = value;
    }

    public Double getKey() {
        return key;
    }

    public void setKey(Double key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("[ ").append(key).append(" , ").append(value).append(" ]").toString();
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		 key = in.readDouble();
	    value = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble((Double) key);
	    out.writeLong((Long) value);		
	}

}