package bigdata.sea.suggest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MutualFriendWritable implements Writable {
	private long userID;
	private long mutualFriendID;

	public MutualFriendWritable() {
		this(-1L, -1L);
	}

	public MutualFriendWritable(long userID, long mutualFriendID) {
		super();
		this.userID = userID;
		this.mutualFriendID = mutualFriendID;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.userID = input.readLong();
		this.mutualFriendID = input.readLong();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(userID);
		output.writeLong(mutualFriendID);
	}

	public long getUserID() {
		return userID;
	}

	public void setUserID(long userID) {
		this.userID = userID;
	}

	public long getMutualFriendID() {
		return mutualFriendID;
	}

	public void setMutualFriendID(long mutualFriendID) {
		this.mutualFriendID = mutualFriendID;
	}

	@Override
	public String toString() {
		return "MutualFriendWritable [userID=" + userID + ", mutualFriendID=" + mutualFriendID + "]";
	}

}
