package de.saschapeukert;

import java.util.Collections;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class MapResult {

    // use apoc?!
    private static final MapResult EMPTY = new MapResult(Collections.emptyMap());
    public final Map<String, Object> value;

    public static MapResult empty() {
        return EMPTY;
    }

    public MapResult(Map<String, Object> value) {
        this.value = value;
    }
}
