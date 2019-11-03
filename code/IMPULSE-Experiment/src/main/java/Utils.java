import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());


        Map<K, V> result = new LinkedHashMap<>();
        for(int i = list.size()-1; i >= 0; i--)
            result.put(list.get(i).getKey(), list.get(i).getValue());

//        for (Map.Entry<K, V> entry : list)

        return result;
    }
}
