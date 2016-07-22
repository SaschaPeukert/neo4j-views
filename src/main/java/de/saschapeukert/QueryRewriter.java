package de.saschapeukert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sascha Peukert on 03.07.2016.
 */
public class QueryRewriter {

    /**
     * The keywords according to the railroad from https://github.com/opencypher/openCypher that can be encountered after
     * CREATE PathExpression
     * Railroad: https://s3.amazonaws.com/artifacts.opencypher.org/railroad/Cypher.html
     */
    private static String[] cypherKeywords = {"SET ","DELETE ","REMOVE ","UNWIND ","FOREACH ", "RETURN ", " MATCH ", "WHERE ",
            "OPTIONAL", "DETACH ", "WITH ", "CREATE ", "MERGE ", ";"};

    public String rewrite(String statement){
        statement = replaceStringFromStatement(statement,"  "," "); // replace useless whitespace
        statement = replacePathWithVirtualPaths(statement,extractVirtualPart(statement));
        // remove VIRTUAL
        statement = replaceStringFromStatement(statement,"CREATE VIRTUAL ","CREATE ");

        return statement;
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

    public String replacePathWithVirtualPaths(String statement, List<ExpressionReplacement> listOfExpressionReplacements){
        for(int i = listOfExpressionReplacements.size()-1; i>=0; i--){
            ExpressionReplacement p  = listOfExpressionReplacements.get(i);
            statement = statement.substring(0,p.getStartPos()) + p.getNewExpression()
                    + statement.substring(p.getEndPos()+1);

        }
        return statement;
    }

    public List<ExpressionReplacement> extractVirtualPart(String statement){
        List<ExpressionReplacement> returnList = new ArrayList<>(); // Multiple Create Virtual possible!

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
            returnList.add(new ExpressionReplacement(path,posStart+diffSum));
            path = createVirtualString + path;

            diffSum = diffSum + statement.length() - (statement.substring(0,statement.toUpperCase().indexOf(path.toUpperCase())) +
                    statement.substring(statement.toUpperCase().indexOf(path.toUpperCase()) + path.length())).length();

            statement =statement.substring(0,statement.toUpperCase().indexOf(path.toUpperCase())) +
                    statement.substring(statement.toUpperCase().indexOf(path.toUpperCase()) + path.length());
        }
        return returnList;
    }
}
