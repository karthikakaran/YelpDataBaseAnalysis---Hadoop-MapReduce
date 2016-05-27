import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class SortTopTen{
	static <K extends Comparable, V extends Comparable> Map<String, Integer> sortByValues(Map<String, Integer> map) {
		List<Map.Entry<String, Integer>> entries = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				if (o2.getValue() < o1.getValue())
					return -1;
				else if (o2.getValue() > o1.getValue())
					return 1;
				else
					return 0;
			}
		});
		java.util.Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
