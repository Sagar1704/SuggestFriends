package bigdata.sea.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SuggestReducer extends
		Reducer<LongWritable, MutualFriendWritable, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable key,
			Iterable<MutualFriendWritable> values, Context context)
					throws IOException, InterruptedException {
		super.reduce(key, values, context);

		final Map<Long, List<String>> mutualFriends = new HashMap<Long, List<String>>();

		for (MutualFriendWritable value : values) {
			Long recommendedFriendID = value.getRecommendedFriendID();
			Long mutualFriendID = value.getMutualFriendID();

			if (mutualFriends.containsKey(recommendedFriendID)) {
				if (mutualFriends.get(recommendedFriendID) != null) {
					if (mutualFriendID == -1L) {
						mutualFriends.put(recommendedFriendID, null);
					} else {
						mutualFriends.get(recommendedFriendID)
								.add("" + mutualFriendID);
					}
				}

			} else {
				if (mutualFriendID == -1L) {
					mutualFriends.put(recommendedFriendID, null);
				} else {
					ArrayList<String> mutualFriendsList = new ArrayList<String>();
					mutualFriendsList.add("" + mutualFriendID);
					mutualFriends.put(recommendedFriendID, mutualFriendsList);
				}
			}
		}

		SortedMap<Long, List<String>> sortedMutualFriends = new TreeMap<Long, List<String>>(
				new Comparator<Long>() {

					@Override
					public int compare(Long key1, Long key2) {
						if (mutualFriends.get(key1).size() > mutualFriends
								.get(key2).size()) {
							return -1;
						} else if (mutualFriends.get(key1)
								.size() == mutualFriends.get(key2).size()
								&& key1 < key2) {
							return -1;
						} else {
							return 1;
						}
					}
				});

		for (Map.Entry<Long, List<String>> entry : mutualFriends.entrySet()) {
			if (entry.getValue() != null) {
				sortedMutualFriends.put(entry.getKey(), entry.getValue());
			}
		}

		String recommendation = "";
		boolean first = true;
		for (SortedMap.Entry<Long, List<String>> entry : sortedMutualFriends
				.entrySet()) {
			if (first) {
				recommendation += entry.getKey().toString() + " -- "
						+ entry.getValue();
				first = false;
			} else {
				recommendation += ", " + entry.getKey().toString() + " -- "
						+ entry.getValue();
			}
		}

		context.write(key, new Text(recommendation));
	}

}
