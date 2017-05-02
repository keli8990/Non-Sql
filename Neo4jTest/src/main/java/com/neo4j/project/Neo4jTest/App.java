package com.neo4j.project.Neo4jTest;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;

import com.neo4j.query.Neo4JComplexQuery;
import com.neo4j.query.Neo4JSimpleQuery;
import com.neo4j.util.LoadData;

public class App 
{
	
    public static void main( String[] args ) throws Exception
    {
    	  Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "221499kjpp" ) );
          Session session = driver.session();
         
//          String t = "80's";
//          boolean bool = t.contains("'");
//          t.replaceAll("'"," ");
//          System.out.println( t.replaceAll("'",""));
          
          Neo4JSimpleQuery sq = new Neo4JSimpleQuery();
//          sq.simpleQuerySecond(session, "MALICE MIZER");  //realized by both Java and shell command
          
          Neo4JComplexQuery cq = new Neo4JComplexQuery();
//          cq.complexQueryFirst(session);  //realized by both Java and shell command
//          cq.complexQuerySecond(session, "浜崎あゆみ");
//          cq.complexQueryThird(session, 375);
//          cq.complexQueryFourth(session, "浜崎あゆみ");
          
         

          session.close();
          driver.close();
    }
}
