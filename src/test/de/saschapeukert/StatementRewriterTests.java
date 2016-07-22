package de.saschapeukert;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Created by Sascha Peukert on 03.07.2016.
 */
public class StatementRewriterTests {

    private static QueryRewriter sut;

    @BeforeClass
    public static void setUp() throws Exception {
        sut = new QueryRewriter();
    }

    @Test
    public void extractVirtualPartShouldWorkProperly(){
        String count ="MATCH(n) RETURN COUNT(n)";
        String oneVirtual = "CREATE VIRTUAL (n:Test) RETURN n";
        String threeVirtual = "CREATE VIRTUAL (n:Test) CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL m-[:WROTE]->n RETURN n";
        String oneVirtualoneReal = "CREATE VIRTUAL (n:Test) CREATE (m:Person) RETURN n,m";

        List<ExpressionReplacement> list =sut.extractVirtualPart(count);
        Assert.assertEquals("Querys without CREATE VIRTUAL should not be appear in the list at all",0,list.size());

        list =sut.extractVirtualPart(oneVirtual);
        Assert.assertEquals("Failed to recognize Statement: " + oneVirtual,1,list.size());
        Assert.assertEquals("Failed to extract Statement: " + oneVirtual,"(n:Test)",list.get(0).getOriginalExpression());

        Assert.assertEquals("Wrong Startposition of ExpressionReplacement for (n:Test)",15,list.get(0).getStartPos());
        Assert.assertEquals("Wrong Endposition of ExpressionReplacement for (n:Test)",22,list.get(0).getEndPos());

        list =sut.extractVirtualPart(threeVirtual);
        Assert.assertEquals("Failed to recognize Statement: " + threeVirtual,3,list.size());
        Assert.assertEquals("Failed to extract first path in Statement: " + threeVirtual,"(n:Test)",list.get(0)
                .getOriginalExpression());
        Assert.assertEquals("Failed to extract second path in Statement: " + threeVirtual,"(m:Person)",list.get(1)
                .getOriginalExpression());
        Assert.assertEquals("Failed to extract third path in Statement: " + threeVirtual,"m-[:WROTE]->n",list.get(2)
                .getOriginalExpression());

        Assert.assertEquals("Wrong Startposition of ExpressionReplacement for (n:Test)",15,list.get(0).getStartPos());
        Assert.assertEquals("Wrong Endposition of ExpressionReplacement for (n:Test)",22,list.get(0).getEndPos());
        Assert.assertEquals("Wrong Startposition of ExpressionReplacement for (m:Person)",39,list.get(1).getStartPos());
        Assert.assertEquals("Wrong Endposition of ExpressionReplacement for (m:Person)",48,list.get(1).getEndPos());
        Assert.assertEquals("Wrong Startposition of ExpressionReplacement for m-[:WROTE]->n",74,list.get(2).getStartPos());
        Assert.assertEquals("Wrong Endposition of ExpressionReplacement for m-[:WROTE]->n",86,list.get(2).getEndPos());

        list =sut.extractVirtualPart(oneVirtualoneReal);
        Assert.assertEquals("Failed to recognize Statement: " + oneVirtualoneReal,1,list.size());
        Assert.assertEquals("Failed to extract Statement: " + oneVirtualoneReal,"(n:Test)",list.get(0)
                .getOriginalExpression());
    }

    @Test
    public void removeWhitespaceShouldWorkFine(){
        String count ="MATCH (n) RETURN COUNT (n)";
        String blownCount = "MATCH (n)      RETURN    COUNT (n)";

        Assert.assertEquals(count,sut.replaceStringFromStatement(blownCount,"  "," "));
    }

    @Test
    public void replacePathWithVirtualPathsShouldWork(){
        String threeVirtual = "CREATE VIRTUAL (n:Test) CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL m-[:WROTE]->n RETURN n";
        List<ExpressionReplacement> list =sut.extractVirtualPart(threeVirtual);

        String result = sut.replacePathWithVirtualPaths(threeVirtual,list);
        String inject = "{" + BaselineProcedure.PROPERTYKEY + ":true}";
        String correctResult = "CREATE VIRTUAL (n:Test"+inject+") CREATE VIRTUAL (m:Person"+inject+") WITH n,m create VIRTUAL m-[:WROTE"+inject+"]->n RETURN n";

        Assert.assertEquals(correctResult,result);
    }

    @Test
    public void replaceCreateVirtualWithCreateShouldWork(){
        String teststring ="CREATE (n:Person) WITH n CREATE VIRTUAL (t:TEST) CREATE VIRTUAL n-[:WROTE]->t";
        String resultString = "CREATE (n:Person) WITH n CREATE (t:TEST) CREATE n-[:WROTE]->t";

        Assert.assertEquals(resultString,sut.replaceStringFromStatement(teststring,"CREATE VIRTUAL ","CREATE "));
    }
}
