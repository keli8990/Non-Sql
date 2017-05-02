package com.mongodb.app;

import com.mongodb.dataload.DataLoad;
import com.mongodb.query.ComplexQuery;
import com.mongodb.query.SimpleQuery;

/**
 * MongoDB query. Load data in the package named com.mongodb.dataload.DataLoad
 * Simple query in com.mongodb.query.SimpleQuery Complex query in
 * com.mongodb.query.ComplexQuery Right click --> Run As --> Java Application
 * 
 * @author Kejin Li
 *
 */
public class MongoApp {

	public static void main(String argv[]) throws Exception {
		/*
		 * Load Data
		 */
		DataLoad dl = new DataLoad();
		// dl.loadTagData("project"); //Step 1
		// dl.loadMediaData("project"); //Step 2
		// dl.loadMediaTagData("project"); //Step 3
		// dl.loadMediaUserData("project"); //Step 4

		SimpleQuery sq = new SimpleQuery();
		/*
		 * Simple query can be executed below, the fourth query and the second
		 * one have been implemented by script
		 */
		 sq.queryFirst("project", 2);

//		 sq.queryThird("project", "Behemoth");

		/*
		 * Complex query can be executed below, the first and the second one
		 * have been implemented by script
		 */
		ComplexQuery cq = new ComplexQuery();
//		 cq.queryComplexThird("project", 375);
//		 cq.queryComplexFourth("project", "Behemoth");

	}
}
