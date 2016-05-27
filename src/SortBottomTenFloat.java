import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class SortBottomTenFloat{
	static <K extends Comparable, V extends Comparable> Map<String, Float> sortByValues(Map<String, Float> map) {
		List<Map.Entry<String, Float>> entries = new LinkedList<Map.Entry<String, Float>>(map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, Float>>() {
			public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
				if (o2.getValue() > o1.getValue())
					return -1;
				else if (o2.getValue() < o1.getValue())
					return 1;
				else
					return 0;
			}
		});
		java.util.Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		for (Entry<String, Float> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
