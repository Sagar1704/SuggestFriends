package bigdata.sea.suggest;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
		final HashMap<Long, HashSet<Long>> recommendedFriends = new HashMap<Long, HashSet<Long>>();

		for (Iterator<MutualFriendWritable> iterator = values
				.iterator(); iterator.hasNext();) {
			MutualFriendWritable value = (MutualFriendWritable) iterator.next();

			Long recommendedFriendID = value.getRecommendedFriendID();
			Long mutualFriendID = value.getMutualFriendID();

			if (recommendedFriends.containsKey(recommendedFriendID)) {
				if (recommendedFriends.get(recommendedFriendID) != null) {
					if (mutualFriendID == -1L) {
						recommendedFriends.put(recommendedFriendID, null);
					} else {
						recommendedFriends.get(recommendedFriendID)
								.add(mutualFriendID);
					}
				}
			} else {
				if (mutualFriendID == -1L) {
					recommendedFriends.put(recommendedFriendID, null);
				} else {
					HashSet<Long> mutualFriendsList = new HashSet<Long>();
					mutualFriendsList.add(mutualFriendID);
					recommendedFriends.put(recommendedFriendID,
							mutualFriendsList);
				}
			}
		}

		SortedMap<Long, HashSet<Long>> sortedRecommendedFriends = new TreeMap<Long, HashSet<Long>>(
				new Comparator<Long>() {

					@Override
					public int compare(Long key1, Long key2) {
						int size1 = recommendedFriends.get(key1).size();
						int size2 = recommendedFriends.get(key2).size();
						if (size1 > size2) {
							return -1;
						} else if (size1 == size2 && key1 < key2) {
							return -1;
						} else {
							return 1;
						}
					}
				});

		for (HashMap.Entry<Long, HashSet<Long>> entry : recommendedFriends
				.entrySet()) {
			if (entry.getValue() != null) {
				sortedRecommendedFriends.put(entry.getKey(), entry.getValue());
			}
		}

		String recommendation = "";
		boolean first = true;
		int count = 0;
		for (HashMap.Entry<Long, HashSet<Long>> entry : sortedRecommendedFriends
				.entrySet()) {
			if (count == 10)
				break;
			if (first) {
				if (entry != null && entry.getKey() != null
						&& entry.getValue() != null) {
					recommendation = entry.getKey().toString();
					first = false;
				}
			} else {
				if (entry != null && entry.getKey() != null
						&& entry.getValue() != null) {
					recommendation += "," + entry.getKey().toString();
				}
			}
			count++;
		}

		context.write(key, new Text(recommendation));
	}

}
