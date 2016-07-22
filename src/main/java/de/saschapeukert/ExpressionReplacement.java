package de.saschapeukert;

/**
 * Created by Sascha Peukert on 23.06.2016.
 */
public class ExpressionReplacement {

    private String originalExpression;
    private String newExpression;
    private int startPos; // Startposition in original Querystring
    private int endPos; // Endposition in original Querystring

    public String getOriginalExpression(){
        return originalExpression;
    }

    public String getNewExpression(){
        return newExpression;
    }

    public int getStartPos(){
        return startPos;
    }

    public int getEndPos(){
        return endPos;
    }

    public ExpressionReplacement(String originalExpression, int start){
        this.originalExpression = originalExpression;
        this.startPos = start;
        this.endPos = start + originalExpression.length()-1;

        enhanceExpression();
    }

    // enhances the path with the marking property where it is necessary
    public void enhanceExpression(){
        // Every new Rel/Node in the virtual Path will be marked
        // (n:LABEL) || (:LABEL) -> new Node
        // [:REL] || [r:REL] -> new Rel
        newExpression = injectVirtualProperties(originalExpression,true);
        newExpression = injectVirtualProperties(newExpression,false);

    }

    public String injectVirtualProperties(String s, boolean node){
        int pos=0;
        int posOpen;
        int posEnd;

        while(pos<s.length()-1) {

            if (node) {
                // NODE
                posOpen = s.indexOf("(", pos);
                posEnd = s.indexOf(")", posOpen);
            } else {
                // REL
                posOpen = s.indexOf("[", pos);
                posEnd = s.indexOf("]", posOpen);
            }

            if (posOpen == -1) {
                return s;
            }
            if (posEnd == -1) {
                return s; // that would be a syntax error!
            }

            int possibleDoublepoint = s.indexOf(":",posOpen);

            if(node) {
                int additionalBracket = s.indexOf("(", posOpen+1);
                if (possibleDoublepoint >additionalBracket && additionalBracket!=-1) {
                    // surrounding ()
                    pos = s.indexOf("(", posOpen+1);
                    continue;
                }
            }
            if(possibleDoublepoint == -1 ){
                return s;
            }

            if (possibleDoublepoint < posEnd) {
                // only if : between posOpen and posEnd

                int posPropOpen = s.indexOf("{", posOpen);
                int posPropEnd;

                if (posPropOpen != -1 && posPropOpen < posEnd) {
                    // there is {
                    posPropEnd = s.indexOf("}", posPropOpen); // there musst be one, if not error

                    if (posPropEnd == -1 || posPropEnd > posEnd) {
                        // ERROR
                        return s;
                    }

                    String inject = "," + BaselineProcedure.PROPERTYKEY + ":true}";
                    pos = posPropEnd + inject.length() ;
                    s = s.substring(0, posPropEnd) + inject + s.substring(posPropEnd + 1);
                } else {
                    // introduce {} too
                    String inject = "{" + BaselineProcedure.PROPERTYKEY + ":true}";
                    pos = posEnd + inject.length() ;
                    s = s.substring(0, posEnd) + inject + s.substring(posEnd);
                }
            } else{
                pos = posEnd;
            }
        }
        return s;
    }
}
