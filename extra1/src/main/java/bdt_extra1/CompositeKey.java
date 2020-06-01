package bdt_extra1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	public String stationId;
	public int temp;

	public CompositeKey() {
	}

	public CompositeKey(String stationId, int temp) {
		super();
		this.set(stationId, temp);
	}

	public void set(String stationId, int temp) {
		this.stationId = (stationId == null) ? "" : stationId;
		this.temp = temp;
	}

//	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stationId);
		out.writeInt(temp);
	}

//	@Override
	public void readFields(DataInput in) throws IOException {
		stationId = in.readUTF();
		temp = in.readInt();
	}

//	@Override
	public int compareTo(CompositeKey o) {
		int stationIdCmp = stationId.toLowerCase().compareTo(o.stationId.toLowerCase());
		if (stationIdCmp != 0) {
			return stationIdCmp;
		} else {
			return Integer.compare( o.temp, temp);
		}
	}
}
