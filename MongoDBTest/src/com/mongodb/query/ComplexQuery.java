package com.mongodb.query;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ComplexQuery {
	/**
	 * find the top 5 artists ranked by the number of users listening to it
	 * 
	 * @param dbName
	 * @throws Exception
	 *             db.getCollection('artists').aggregate( [ { $project: { name:
	 *             1, numberOfListeners: { $size: "$user_info" } } },
	 *             {$sort:{numberOfListeners:-1}}, { $limit : 5 } ] )
	 */
	

	/**
	 * given an artist name, find the top 20 tags assigned to it. The tags are
	 * ranked by the number of times it has been assigned to this artist
	 * 
	 * @param dbName
	 * @param artist_name
	 * @throws Exception
	 *             db.getCollection('artistsTag').aggregate([
	 *             {$match:{artist_name:"äºÆé¤¢¤æ¤ß"}},
	 *             {$group:{_id:{"tag_id":"$tag_id","tag_value":"$tag_value","artist_id":"$artist_id","artist_name":"$artist_name"},
	 *             "totalTags":{$sum:1}}}, {$sort:{"totalTags":-1}}, {$limit:20}
	 *             ])
	 */
	

	/**
	 * given a user id, find the top 5 artists listened by his friends but not
	 * him. We rank artists by the sum of friends¡¯ listening counts of the
	 * artist.
	 * 
	 * @param dbName
	 * @param userid
	 * @throws Exception
	 */
	public void queryComplexThird(String dbName, int userid) throws Exception {
		long startTime=System.currentTimeMillis();
		MongoClient mongoClient = new MongoClient("localhost");
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection coll = db.getCollection("users");
		MongoCollection collection = db.getCollection("artists");
		Document myDoc = (Document) coll.find(eq("id", userid)).first();
		JSONArray resultArray = new JSONArray("[" + myDoc.toJson() + "]");
		JSONObject resultObj = resultArray.optJSONObject(0);

		JSONArray artistArray = (JSONArray) resultObj.get("artist");
		List<Integer> artistList = new ArrayList<Integer>();

		// add user's artist
		for (int i = 0; i < artistArray.length(); i++) {
			JSONObject artistObj = artistArray.optJSONObject(i);
			artistList.add(Integer.parseInt(artistObj.get("mediaid").toString()));
		}

		// get all the friends of user with userid
		JSONArray friendArray = (JSONArray) resultObj.get("friends");
		List<Integer> mediaIdForFriends = new ArrayList<Integer>();
		List<Integer> mediaWeightForFriends = new ArrayList<Integer>();
		for (int i = 0; i < friendArray.length(); i++) {
			Document friendDoc = (Document) coll.find(eq("id", friendArray.get(i))).first();
			// System.out.println( friendDoc.toJson()); //get user's friend
			// profile
			JSONArray subFriendArray = new JSONArray("[" + friendDoc.toJson() + "]");
			JSONObject subFriendObj = subFriendArray.optJSONObject(0);
			// get all the artists listened by user's friends
			JSONArray artarray = new JSONArray(subFriendObj.get("artist").toString());

			for (int j = 0; j < artarray.length(); j++) {
				JSONArray single_artarray = new JSONArray("[" + artarray.get(j).toString() + "]");

				JSONObject artObj = single_artarray.optJSONObject(0);

				mediaIdForFriends.add(Integer.parseInt(artObj.get("mediaid").toString()));
				mediaWeightForFriends.add(Integer.parseInt(artObj.get("mediaweight").toString()));
			}
		}

		int weight_temp = 0;
		Map<Integer, Integer> artWeightDic = new HashMap<Integer, Integer>();
		artWeightDic.put(0, 0);
		for (int i = 0; i < mediaIdForFriends.size(); i++) {
			if (artWeightDic.get(mediaIdForFriends.get(i)) == null) {
				artWeightDic.put(mediaIdForFriends.get(i), mediaWeightForFriends.get(i));
			} else {
				weight_temp = artWeightDic.get(mediaIdForFriends.get(i)) + mediaWeightForFriends.get(i);

				artWeightDic.put(mediaIdForFriends.get(i), weight_temp);
			}
		}

		// remove the duplicate value
		Iterator<Integer> iter = artWeightDic.keySet().iterator();
		while (iter.hasNext()) {
			int k = iter.next();
			for (int i = 0; i < artistList.size(); i++) {
				if (k == artistList.get(i)) {
					iter.remove();
					artWeightDic.remove(k);

				}
			}
		}

		List<Map.Entry<Integer, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<Integer, Integer>>(artWeightDic.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> mapping1, Map.Entry<Integer, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});
		List<Integer> topArtists = new ArrayList<Integer>();
		for (Map.Entry<Integer, Integer> mapping : mappingList) {
			topArtists.add(mapping.getKey());
		}
		// select first top 5 artists
		Document artistDoc;
		for (int i = 0; i < 5; i++) {
			artistDoc = (Document) collection.find(eq("artist_id", topArtists.get(i).toString())).first();
			System.out.println(artistDoc.toJson());
		}
		long endTime=System.currentTimeMillis();
		System.out.println("execution time: "+(endTime-startTime)+"ms");  
	}

	/**
	 * given an artist name, find the top 5 similar artists. Here similarity
	 * between a pair of artists is defined by the number of unique users that
	 * have listened both. The higher the number, the more similar the two
	 * artists are.
	 * 
	 * @param dbName
	 * @param artist_name
	 * @throws Exception
	 */

	public void queryComplexFourth(String dbName, String artist_name) throws Exception {
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

		List<String> userid_list = new ArrayList<String>();

		for (int i = 1; i < userArray.length(); i++) {
			JSONObject listenerObj = userArray.optJSONObject(i);
			userid_list.add(listenerObj.get("userid").toString());
		}

		List<String> total_artist_list = new ArrayList<String>();

		Document single_user;
		for (int i = 0; i < userid_list.size(); i++) {
			single_user = (Document) coll.find(eq("id", Integer.parseInt(userid_list.get(i)))).first();
			JSONArray singleUserArray = new JSONArray("[" + single_user.toJson() + "]");
			JSONObject singleUserObj = singleUserArray.optJSONObject(0);

			JSONArray singleUserArtistArray = new JSONArray(singleUserObj.get("artist").toString());

			// get user's artistId
			for (int j = 0; j < singleUserArtistArray.length(); j++) {
				JSONArray single_artarray = new JSONArray("[" + singleUserArtistArray.get(j).toString() + "]");

				JSONObject artObj = single_artarray.optJSONObject(0);
				total_artist_list.add(artObj.get("mediaid").toString());
			}
		}

		Map<String, Integer> statistic_artist_map = new HashMap<String, Integer>();
		statistic_artist_map.put(null, 0);
		int art_temp = 0;
		for (int i = 0; i < total_artist_list.size(); i++) {
			if (statistic_artist_map.get(total_artist_list.get(i)) == null) {
				statistic_artist_map.put(total_artist_list.get(i), 1);
			} else {
				art_temp = statistic_artist_map.get(total_artist_list.get(i)) + 1;
				statistic_artist_map.put(total_artist_list.get(i), art_temp);
			}
		}

		// sort the map value according to map value (desc)
		List<Map.Entry<String, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<String, Integer>>(statistic_artist_map.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> mapping1, Map.Entry<String, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});
		List<String> samilarArtists = new ArrayList<String>();
		for (Map.Entry<String, Integer> mapping : mappingList) {
			samilarArtists.add(mapping.getKey());
		}
		Document samilarDoc;
		for (int i = 0; i < 6; i++) {
			if (samilarArtists.get(i).equals(artist_id)) {
				continue;
			}
			samilarDoc = (Document) collection.find(eq("artist_id", samilarArtists.get(i).toString())).first();
			System.out.println(samilarDoc.toJson());
		}
		long endTime=System.currentTimeMillis();
		System.out.println("execution time: "+(endTime-startTime)+"ms"); 
	}
}
