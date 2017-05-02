package com.mongodb.query;

import static com.mongodb.client.model.Filters.eq;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryOperators;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SimpleQuery {

	/**
	 * Simple query 1: given a user id, find all artists the user¡¯s friends
	 * listen.
	 * 
	 * @param dbName
	 * @param userid
	 * @throws Exception
	 */
	public void queryFirst(String dbName, int userid) throws Exception {
		
		long startTime=System.currentTimeMillis(); 
		
		MongoClient mongoClient = new MongoClient("localhost");
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection coll = db.getCollection("users");
		MongoCollection collection = db.getCollection("artists");
		Document myDoc = (Document) coll.find(eq("id", userid)).first();

		/*
		 * Execute when u want to use $expalin
		 */
		/*
		 * MongoClient serverConnection = new MongoClient("localhost",27017); DB
		 * dbm = serverConnection.getDB("project"); DBObject searchObject = new
		 * BasicDBObject(); searchObject.put("id", userid);
		 * System.out.println(dbm.getCollection("users").find(searchObject).
		 * explain());
		 */

		JSONArray resultArray = new JSONArray("[" + myDoc.toJson() + "]");
		JSONObject resultObj = resultArray.optJSONObject(0);

		JSONArray friendArray = (JSONArray) resultObj.get("friends");
		List<Integer> mediaIdForFriends = new ArrayList<Integer>();
		for (int i = 0; i < friendArray.length(); i++) {
			Document friendDoc = (Document) coll.find(eq("id", friendArray.get(i))).first();
			JSONArray subFriendArray = new JSONArray("[" + friendDoc.toJson() + "]");
			JSONObject subFriendObj = subFriendArray.optJSONObject(0); // get
																		// each
																		// friend's
																		// artists
			JSONArray artarray = new JSONArray(subFriendObj.get("artist").toString()); // get
																						// the
																						// artists
																						// in
																						// two-dimensional
																						// array

			for (int j = 0; j < artarray.length(); j++) {
				JSONArray single_artarray = new JSONArray("[" + artarray.get(j).toString() + "]");

				JSONObject artObj = single_artarray.optJSONObject(0);
				mediaIdForFriends.add(Integer.parseInt(artObj.get("mediaid").toString()));// get
																							// each
																							// artaist's
																							// id
			}
		}

		HashSet h = new HashSet(mediaIdForFriends);
		mediaIdForFriends.clear();
		mediaIdForFriends.addAll(h);
		Collections.sort(mediaIdForFriends);

		for (int i = 0; i < mediaIdForFriends.size(); i++) {
			Document artistDoc = (Document) collection.find(eq("artist_id", mediaIdForFriends.get(i).toString()))
					.first();

			System.out.println("Artist:" + artistDoc.toJson());
		}
		System.out.println(mediaIdForFriends.size());
		long endTime=System.currentTimeMillis();
		System.out.println("execution time: "+(endTime-startTime)+"ms");  
	}

	/**
	 * Simple query 2: given an artist name, find the most recent 10 tags that
	 * have been assigned to it.
	 * 
	 * db.getCollection('artistsTag').find({artist_name:"MALICE
	 * MIZER"}).sort({"tag_timestamp":-1}).limit(10)
	 * 
	 * @param dbName
	 * @param artist_name
	 * @throws Exception
	 */
	public void querySecond(String dbName, String artist_name) throws Exception {
		MongoClient mongoClient = new MongoClient("localhost");
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection collection = db.getCollection("artists");
		MongoCollection collectionarttag = db.getCollection("artistsTag");

		Document artistDoc = (Document) collection.find(eq("name", artist_name)).first();
		String artist_id = (String) artistDoc.get("artist_id");
		System.out.println("artist_id:" + artist_id); // get the artist_id
														// according to the
														// artist_name
		List<String> jsonArtTag = new ArrayList<String>();
		FindIterable<Document> artTagDoc = collectionarttag.find(eq("artist_id", artist_id))
				.sort(new Document("tag_timestamp", -1)).limit(10);
		for (Document result : artTagDoc) {
			jsonArtTag.add(result.toJson());
		}

		JSONArray single_artarray;
		for (int i = 0; i < jsonArtTag.size(); i++) {
			single_artarray = new JSONArray("[" + jsonArtTag.get(i) + "]");
			JSONObject tagidObj = single_artarray.optJSONObject(0);
			System.out.println("Tagid:" + tagidObj.get("tag_id") + " --- TagValue:" + tagidObj.get("tag_value"));
		}
	}

	/**
	 * given an artist name, find the top 10 users based on their respective
	 * listening counts of this artist. Display both the user id and the
	 * listening count
	 * 
	 * @param dbName
	 * @param artist_name
	 * @throws Exception
	 */
	public void queryThird(String dbName, String artist_name) throws Exception {
		
		long startTime=System.currentTimeMillis(); 
		
		MongoClient mongoClient = new MongoClient("localhost");
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection coll = db.getCollection("users");
		MongoCollection collection = db.getCollection("artists");
		Document artistDoc = (Document) collection.find(eq("name", artist_name)).first();
		JSONArray artistArray = new JSONArray("[" + artistDoc.toJson() + "]");
		JSONObject artistObj = artistArray.optJSONObject(0);

		String artist_id = artistObj.get("artist_id").toString();

		// get the userid who listen to this artist
		JSONArray userArray = new JSONArray(artistObj.get("user_info").toString());

		Map<String, Integer> user_weight_map = new HashMap<String, Integer>();
		for (int i = 1; i < userArray.length(); i++) {
			JSONObject listenerObj = userArray.optJSONObject(i);
			user_weight_map.put(listenerObj.get("userid").toString(),
					Integer.parseInt(listenerObj.get("weight").toString()));
		}

		// sort the map according to the weight
		List<Map.Entry<String, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<String, Integer>>(user_weight_map.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> mapping1, Map.Entry<String, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});

		if (mappingList.size() < 11) {
			for (Map.Entry<String, Integer> mapping : mappingList) {
				System.out.println("UserId:" + mapping.getKey() + "---Weight:" + mapping.getValue());
			}
		} else {
			for (int i = 0; i < 10; i++) {
				System.out.println(
						"UserId:" + mappingList.get(i).getKey() + "---Weight:" + mappingList.get(i).getValue());
			}
		}
		
		long endTime=System.currentTimeMillis();
		System.out.println("execution time: "+(endTime-startTime)+"ms");  
	}

	/**
	 * given a user id, find the most recent 10 artists the user has assigned
	 * tag to.
	 * db.getCollection('artistsTag').find({user_id:"3"}).sort({"tag_timestamp":-1}).limit(10)
	 * 
	 * 
	 */
	
	

}
