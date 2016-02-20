package bigdata.sea.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SuggestMapper
		extends Mapper<LongWritable, Text, LongWritable, MutualFriendWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String profile[] = value.toString().split("\t");
		Long userId = Long.parseLong(profile[0]);
		List<Long> friends = new ArrayList<Long>();

		if (profile.length == 2) {
			StringTokenizer tokens = new StringTokenizer(profile[1], ",");
			while (tokens != null && tokens.hasMoreTokens()) {
				Long friend = Long.parseLong(tokens.nextToken());
				friends.add(friend);
				context.write(new LongWritable(userId),
						new MutualFriendWritable(friend, -1L));
			}

			System.out.println("Sagar::friends-size::" + friends.size());

			for (int friend1 = 0; friend1 < friends.size(); friend1++) {
				for (int friend2 = friend1 + 1; friend2 < friends.size(); friend2++) {
					if (friend1 != friend2) {
						context.write(new LongWritable(friends.get(friend1)),
								new MutualFriendWritable(friends.get(friend2),
										userId));

						context.write(new LongWritable(friends.get(friend2)),
								new MutualFriendWritable(friends.get(friend1),
										userId));
					}
				}
			}
		}
	}
}
