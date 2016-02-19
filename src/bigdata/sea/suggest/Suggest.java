package bigdata.sea.suggest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Suggest {
	public static class SuggestMapper extends
			Mapper<LongWritable, Text, LongWritable, MutualFriendWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
			String profile[] = value.toString().split("\t");
			Long userId = Long.parseLong(profile[0]);
			String[] friends = profile[1].split(",");

			if (profile.length == 2) {
				for (String friendId : friends) {
					context.write(new LongWritable(userId),
							new MutualFriendWritable(friendId, -1L));
				}

				for (int friend1 = 0; friend1 < friends.length; friend1++) {
					for (int friend2 = friend1
							+ 1; friend2 < friends.length; friend2++) {
						context.write(
								new LongWritable(
										Long.parseLong(friends[friend1])),
								new MutualFriendWritable(friends[friend2],
										userId));

						context.write(
								new LongWritable(
										Long.parseLong(friends[friend2])),
								new MutualFriendWritable(friends[friend1],
										userId));
					}
				}
			}
		}

	}

	public static class SuggestReducer extends
			Reducer<LongWritable, MutualFriendWritable, LongWritable, Text> {

		@Override
		protected void reduce(LongWritable key,
				Iterable<MutualFriendWritable> values, Context context)
						throws IOException, InterruptedException {
			super.reduce(key, values, context);
			
			
		}

	}
}
