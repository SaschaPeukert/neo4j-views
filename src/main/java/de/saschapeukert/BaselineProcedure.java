package de.saschapeukert;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.String.join;
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
    public Stream<MapResult> get(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        virtualHandler.setGraphDB(api);
        api.registerTransactionEventHandler(virtualHandler);
        return run(statement,params);
    }

    private Stream<MapResult> run(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return db.execute(compiled(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    private String compiled(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" {`%s`} as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }


    public List<String> extractVirtualPart(String statement){
        List<String> returnList = new ArrayList<>(); // Multiple Create Virtual possible!
        String[] cypherKeywords = {"SET ","DELETE ","REMOVE ","FOREACH ", "RETURN ", " MATCH ", "WHERE ", "OPTIONAL",
        "WITH ", "CREATE ", ";"};
        String createVirtualString = "CREATE VIRTUAL ";
        statement = statement.toUpperCase();
        String path = "";

        // PROBLEM: USELESS WHITESPACE

        while(statement.contains(createVirtualString)){
            // find start
            int pos1 = statement.indexOf(createVirtualString);
            pos1 = pos1 + createVirtualString.length();
            path = statement.substring(pos1); // statement with start after Create virtual
            // find end of path -> looking for next cypher keyword
            int pos_end_min = path.length()-1;
            for(String s:cypherKeywords){
                if(path.contains(s)) {
                    int tempPos = path.indexOf(s) - 1;
                    if (tempPos < pos_end_min)
                        pos_end_min = tempPos;
                }
            }
            path = path.substring(0,pos_end_min);
            returnList.add(path);
            path = createVirtualString + path;

            statement =statement.substring(0,statement.indexOf(path)) +
                    statement.substring(statement.indexOf(path) + path.length());
        }
        return returnList;
    }
}

