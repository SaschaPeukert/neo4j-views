package de.saschapeukert;

/**
 * Created by Sascha Peukert on 23.06.2016.
 */
public class PathReplacement {

    private String originalPathString;
    private String newPathString;
    private int startPos; // Startposition in original Querystring
    private int endPos; // Endposition in original Querystring

    public String getOriginalPathString(){
        return originalPathString;
    }

    public String getNewPathString(){
        return newPathString;
    }

    public int getStartPos(){
        return startPos;
    }

    public int getEndPos(){
        return endPos;
    }

    public PathReplacement(String originalPathString, int start, int end){
        this.originalPathString = originalPathString;
        this.startPos = start;
        this.endPos = end;

        enhancePath();
    }

    // enhances the path with the marking property where it is necessary
    public void enhancePath(){
        // Every new Rel/Node in the virtual Path will be marked
        // (n:LABEL) || (:LABEL) -> new Node
        // [:REL] || [r:REL] -> new Rel
        newPathString = injectVirtualProperties(originalPathString,true);
        newPathString = injectVirtualProperties(newPathString,false);

    }

    public String injectVirtualProperties(String s, boolean node){


        // TODO: finish loop
        int pos=0;
        int posOpen;
        int posEnd;

        if(node){
            // NODE
            posOpen = s.indexOf("(",pos);
            posEnd = s.indexOf(")",posOpen);

        } else{
            // REL
            posOpen = s.indexOf("[",pos);
            posEnd = s.indexOf("]",posOpen);

        }

        if(posOpen==-1){
            return s;
        }
        if(posEnd==-1){
            return s; // that would be a syntax error!
        }

        int possibleDoublepoint = s.indexOf(":");
        if(possibleDoublepoint!=-1 && posOpen<possibleDoublepoint && possibleDoublepoint<posEnd){
            // only if : between posOpen and posEnd

            int posPropOpen = s.indexOf("{",posOpen);
            int posPropEnd;

            if(posPropOpen!=-1&&posPropOpen<posEnd){
                // there is {
                posPropEnd = s.indexOf("}",posPropOpen); // there musst be one, if not error

                if(posPropEnd==-1 || posPropEnd>posEnd){
                    // ERROR
                    return s;
                }

                // posPropsEnd " " + BaselineProcedure.
                String inject = "," + BaselineProcedure.PROPERTYKEY + ":true}";
                s=  s.substring(0,posPropEnd) + inject + s.substring(posPropEnd+1);
            } else{
                // introduce {} too
                String inject = "{" + BaselineProcedure.PROPERTYKEY + ":true}";
                s=  s.substring(0,posEnd) + inject + s.substring(posEnd);
            }

        }



        // search for (

        // search for next )
        // is there : between
        // if so, is there } ?
        // if so, add ,property before that
        // if not, add {property} before )

        // search for [
        // is there : between
        // ...


        return s;
    }

    //TODO: change tests for extract
}
