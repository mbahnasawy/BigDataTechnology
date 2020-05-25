

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ComplexKey implements WritableComparable<ComplexKey>{
	
	String stationID ; 
	int temperature; 

	public ComplexKey() {
	}

	public ComplexKey(String stationID, int temperature) {
		this.stationID = stationID;
		this.temperature = temperature;
	}

	public String getStationID() {
		return stationID;
	}

	public void setStationID(String stationID) {
		this.stationID = stationID;
	}


	public int getTemperature() {
		return temperature;
	}


	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stationID = in.readUTF();
		temperature = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		  out.writeUTF(stationID);
	      out.writeInt(temperature);
		
	}

	@Override
	public int compareTo(ComplexKey o) {
		
		if(this.getStationID().equals(o.getStationID())){
			//temperature in descending order
			return this.getTemperature() > o.getTemperature()? -1:1 ; 
		}else{ // stationID in ascending order
			return this.getStationID().compareTo(o.getStationID());
		}
	}
	
    @Override
    public String toString() { 
        return stationID.substring(0,6) + "-" + stationID.substring(6,11) + "\t" + temperature; 
    }

}
