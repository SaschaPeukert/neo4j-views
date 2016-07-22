package de.saschapeukert;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;

/**
 * Created by Sascha Peukert on 14.06.2016.
 */
public class BaselineProcedure {

    @Context
    public GraphDatabaseService db;
    @Context
    public GraphDatabaseAPI api;

    public static String PROPERTYKEY = "$VIRTUAL$";

    public static NaiveBaselineTransactionEventHandler virtualHandler = new NaiveBaselineTransactionEventHandler();

    @Procedure
    @PerformsWrites
    public Stream<MapResult> runCypher(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        virtualHandler.setGraphDB(api);
        QueryRewriter sr = new QueryRewriter();
        api.registerTransactionEventHandler(virtualHandler);
        Stream<MapResult> res = run(sr.rewrite(statement),params);
        //api.unregisterTransactionEventHandler(virtualHandler);
        return res;
    }

    /*
    This will produce problems, so let it be just for testing

    @Procedure
    public Stream<StringResult> setVirtualPropertyKey(@Name("propertykey") String key){
        PROPERTYKEY = key;
        return getVirtualPropertyKey();
    }
    */
    @Procedure
    public Stream<StringResult> getVirtualPropertyKey(){
        List<StringResult> s = new ArrayList<>(1);
        s.add(new StringResult(PROPERTYKEY));
        return StreamSupport
                .stream( spliteratorUnknownSize( s.iterator(), 0 ), false );
    }

    private Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(compiled(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    private String compiled(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s))
                .collect(toList()));
        return declaration + fragment;
    }

}