package Q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class YearWritable implements WritableComparable<YearWritable>{
	
	String year ; 
	
	public YearWritable() {
	}

	public YearWritable(String year) {
		this.year = year ; 
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		  out.writeUTF(year);
	}

	@Override
	public int compareTo(YearWritable o) {
		return o.getYear().compareTo(this.getYear()) ;
	}
	
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + year.hashCode();
        return result;
      }
    
    @Override
    public String toString() { 
        return year; 
    }

}
