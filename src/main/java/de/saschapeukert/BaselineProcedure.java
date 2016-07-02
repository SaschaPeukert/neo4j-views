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

    /**
     * Keywords according to railroad from https://github.com/opencypher/openCypher
     * Railroad: https://s3.amazonaws.com/artifacts.opencypher.org/railroad/Cypher.html
     */
    private static String[] cypherKeywords = {"SET ","DELETE ","REMOVE ","UNWIND ","FOREACH ", "RETURN ", " MATCH ", "WHERE ",
            "OPTIONAL", "DETACH ", "WITH ", "CREATE ", "MERGE ", ";"};
    // keywords according to railroad from
    // https://s3.amazonaws.com/artifacts.opencypher.org/railroad/Cypher.html

    public static NaiveBaselineTransactionEventHandler virtualHandler = new NaiveBaselineTransactionEventHandler();

    @Procedure
    @PerformsWrites
    public Stream<MapResult> runCypher(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        virtualHandler.setGraphDB(api);

        statement = replaceStringFromStatement(statement,"  "," "); // replace useless whitespace
        statement = replacePathWithVirtualPaths(statement,extractVirtualPart(statement));
        // remove VIRTUAL
        statement = replaceStringFromStatement(statement,"CREATE VIRTUAL ","CREATE ");

        api.registerTransactionEventHandler(virtualHandler);
        Stream<MapResult> res = run(statement,params);
        //api.unregisterTransactionEventHandler(virtualHandler);
        return res;
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

    /**
     *
     * @param statement
     * @param remove Needs to be longer than instead string!
     * @param instead
     * @return
     */
    public String replaceStringFromStatement(String statement, String remove, String instead){
        int idx = statement.toLowerCase().indexOf(remove.toLowerCase());
        while(idx!=-1){
            statement = statement.substring(0,idx) + instead +statement.substring(idx+remove.length());
            idx = statement.toLowerCase().indexOf(remove.toLowerCase());
        }

        return statement;
    }

    public String replacePathWithVirtualPaths(String statement, List<PathReplacement> listOfPathReplacements){
        for(int i=listOfPathReplacements.size()-1;i>=0;i--){
            PathReplacement p  = listOfPathReplacements.get(i);
            statement = statement.substring(0,p.getStartPos()) + p.getNewPathString()
                    + statement.substring(p.getEndPos()+1);

        }
        return statement;
    }

    public List<PathReplacement> extractVirtualPart(String statement){
        List<PathReplacement> returnList = new ArrayList<>(); // Multiple Create Virtual possible!

        String createVirtualString = "CREATE VIRTUAL ";
        String path = "";
        int diffSum =0;

        while(statement.toUpperCase().contains(createVirtualString)){
            // find start
            int posStart = statement.toUpperCase().indexOf(createVirtualString);
            posStart = posStart + createVirtualString.length();
            path = statement.substring(posStart); // statement with start after Create virtual
            // find end of path -> looking for next cypher keyword
            int pos_end_min = path.length()-1;
            for(String s:cypherKeywords){
                if(path.contains(s)) {
                    int tempPos = path.toUpperCase().indexOf(s) - 1;
                    if (tempPos < pos_end_min)
                        pos_end_min = tempPos;
                }
            }
            path = path.substring(0,pos_end_min);
            returnList.add(new PathReplacement(path,posStart+diffSum));
            path = createVirtualString + path;

            diffSum = diffSum + statement.length() - (statement.substring(0,statement.toUpperCase().indexOf(path.toUpperCase())) +
                    statement.substring(statement.toUpperCase().indexOf(path.toUpperCase()) + path.length())).length();

            statement =statement.substring(0,statement.toUpperCase().indexOf(path.toUpperCase())) +
                    statement.substring(statement.toUpperCase().indexOf(path.toUpperCase()) + path.length());
        }
        return returnList;
    }
}