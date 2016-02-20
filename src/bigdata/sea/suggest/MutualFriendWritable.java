package bigdata.sea.suggest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MutualFriendWritable implements Writable {
	private long recommendedFriendID;
	private long mutualFriendID;

	public MutualFriendWritable() {
		this(-1L, -1L);
	}

	public MutualFriendWritable(long recommendedFriendID,
			long mutualFriendID) {
		super();
		this.recommendedFriendID = recommendedFriendID;
		this.mutualFriendID = mutualFriendID;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.recommendedFriendID = input.readLong();
		this.mutualFriendID = input.readLong();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(recommendedFriendID);
		output.writeLong(mutualFriendID);
	}

	public long getRecommendedFriendID() {
		return recommendedFriendID;
	}

	public void setRecommendedFriendID(long recommendedFriendID) {
		this.recommendedFriendID = recommendedFriendID;
	}

	public long getMutualFriendID() {
		return mutualFriendID;
	}

	public void setMutualFriendID(long mutualFriendID) {
		this.mutualFriendID = mutualFriendID;
	}

	@Override
	public String toString() {
		return "MutualFriendWritable [recommendedFriendID="
				+ recommendedFriendID + ", mutualFriendID=" + mutualFriendID
				+ "]";
	}

}
