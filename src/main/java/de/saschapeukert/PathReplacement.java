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
        newPathString = translate(originalPathString,true);
        newPathString = translate(newPathString,false);

    }

    public String translate(String s, boolean node){

        // if node => node
        // if not node => rel

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

    //TODO: change tests for extract and add Test for enhancePath
}
