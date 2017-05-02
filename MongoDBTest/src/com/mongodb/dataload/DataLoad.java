package com.mongodb.dataload;

import static com.mongodb.client.model.Filters.eq;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;

public class DataLoad {
	MongoClient mongoClient = new MongoClient("localhost");

	public void loadTagData(String dbName) throws Exception {

		MongoDatabase db = mongoClient.getDatabase(dbName);

		MongoCollection coll = db.getCollection("tags");
		// clear all data in the collection

		coll.deleteMany(new Document());

		Document doc;

		FileReader reader_tag = new FileReader("D://comp5338Project//project//tags.dat");
		BufferedReader br_tag = new BufferedReader(reader_tag);
		String str_tag = null;
		String[] artarr = null;
		int count_tag = 0;
		while ((str_tag = br_tag.readLine()) != null) {
			if (count_tag == 0) {
				count_tag++;
				continue;
			}

			artarr = str_tag.split("\t");

			doc = new Document().append("tag_id", artarr[0]).append("tag_value", artarr[1]);
			coll.insertOne(doc);

			count_tag++;
		}
	}

	public void loadMediaTagData(String dbName) throws Exception {

		MongoDatabase db = mongoClient.getDatabase(dbName);

		MongoCollection coll = db.getCollection("artistsTag");
		MongoCollection colltag = db.getCollection("tags");
		MongoCollection collartist = db.getCollection("artists");
		
		coll.createIndex(new Document("artist_name", 1));
		coll.createIndex(new Document("user_id", 1));
		coll.createIndex(new Document("tag_timestamp", 1));

		colltag.createIndex(new Document("tag_id", 1));
		collartist.createIndex(new Document("artist_id", 1));
		

		String artist_name = null;
		String artist_url = null;
		String artist_pu = null;
		String tag_name = null;
		// clear all data in the collection
		Document doc;
		Document doc_artist;
		Document doc_tag;
		List<Integer> userListOfTag = new ArrayList<Integer>();

		List<Integer> userArtistTagList = new ArrayList<Integer>();
		List<Integer> userTagList = new ArrayList<Integer>();
		List<Date> tagStampList = new ArrayList<Date>();
		int temptag = 2;

		InputStreamReader tagsreader = new InputStreamReader(
				new FileInputStream("D://comp5338Project//project//user_taggedartists-timestamps.dat"), "UTF-8");

		BufferedReader brtag = new BufferedReader(tagsreader);

		String strtag = null;
		String[] taguserarr = null;
		int count_tag = 0;

		List<String> list_art = new ArrayList<String>();
		List<String> list_tag = new ArrayList<String>();
		List<Date> list_timestamp = new ArrayList<Date>();
		String[] temp_art_array = new String[3];
		StringBuilder sb = new StringBuilder();
		Date date = new Date();
		while ((strtag = brtag.readLine()) != null) {
			if (count_tag == 0) {
				count_tag++;
				continue;
			}

			taguserarr = strtag.split("\t");

			doc_artist = (Document) collartist.find(new Document("artist_id", taguserarr[1])).first();
			if (doc_artist != null) {
				JSONArray artistArray = new JSONArray("[" + doc_artist.toJson() + "]");
				JSONObject artistObj = artistArray.optJSONObject(0);
				artist_name = artistObj.get("name").toString();
				artist_url = artistObj.get("url").toString();
				artist_pu = artistObj.get("pictureURL").toString();
				if (artist_pu.equals(null) || artist_pu.length() <= 0 || artist_pu == "") {
					artist_pu = null;
				}
			}

			doc_tag = (Document) colltag.find(eq("tag_id", taguserarr[2])).first();
			JSONArray tagArray = new JSONArray("[" + doc_tag.toJson() + "]");
			JSONObject tagObj = tagArray.optJSONObject(0);
			tag_name = tagObj.get("tag_value").toString();

			Long timestamp = Long.parseLong(taguserarr[3]);
			String dateStr = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(timestamp));
			DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			date = sdf.parse(dateStr);

			doc = new Document().append("user_id", taguserarr[0]).append("artist_id", taguserarr[1])
					.append("artist_name", artist_name).append("artist_url", artist_url)
					.append("artist_pictureURL", artist_pu).append("tag_id", taguserarr[2])
					.append("tag_value", tag_name).append("tag_timestamp", date);
			coll.insertOne(doc);

		}

	}

	public void loadMediaData(String dbName) throws Exception {

		MongoDatabase db = mongoClient.getDatabase(dbName);

		MongoCollection coll = db.getCollection("artists");
		// clear all data in the collection
		coll.createIndex(new Document("artist_id", 1));
		coll.createIndex(new Document("name", 1));
		coll.deleteMany(new Document());

		Document doc;
		Document art_doc;
		Document art_sub_doc;

		List<String> user_info_array = new ArrayList<String>();
		List<String> tag_info_array = new ArrayList<String>();

		StringBuffer sb = new StringBuffer("");

		InputStreamReader reader = new InputStreamReader(
				new FileInputStream("D://comp5338Project//project//artists.dat"), "UTF-8");
		BufferedReader br = new BufferedReader(reader);
		String str = null;
		String[] artarr = null;
		int count = 0;
		while ((str = br.readLine()) != null) {
			if (count == 0) {
				count++;
				continue;
			}

			artarr = str.split("\t");

			if (artarr.length > 3) {
				doc = new Document().append("artist_id", artarr[0]).append("name", artarr[1]).append("url", artarr[2])
						.append("pictureURL", artarr[3]).append("user_info", user_info_array);
				coll.insertOne(doc);
			} else {
				doc = new Document().append("artist_id", artarr[0]).append("name", artarr[1]).append("url", artarr[2])
						.append("pictureURL", null).append("user_info", user_info_array);
				coll.insertOne(doc);
			}

			count++;
		}

		// Add user info in Artist Collection

		StringBuffer sb_user = new StringBuffer("");

		/**
		 * Create three list for user_id, artist_id, and weight
		 */
		List<String> user_id_list = new ArrayList<String>();
		List<String> artist_id_list = new ArrayList<String>();
		List<String> weight_list = new ArrayList<String>();

		// Read the file
		List<Document> sub_art_list = new ArrayList<Document>();
		FileReader reader_user = new FileReader("D://comp5338Project//project//user_artists.dat");
		BufferedReader br_user = new BufferedReader(reader_user);
		String str_user = null;
		String[] art_user_arr = null;
		int count_user = 0;
		while ((str_user = br_user.readLine()) != null) {
			if (count_user == 0) {
				count_user++;
				continue;
			}

			art_user_arr = str_user.split("\t");
			user_id_list.add(art_user_arr[0]);
			artist_id_list.add(art_user_arr[1]);
			weight_list.add(art_user_arr[2]);

			count_user++;
		}

		// query the specific artist
		for (int i = 0; i < artist_id_list.size(); i++) {

			art_sub_doc = new Document().append("userid", user_id_list.get(i)).append("weight", weight_list.get(i));

			coll.updateOne(new Document("artist_id", artist_id_list.get(i)), Updates.push("user_info", art_sub_doc));

		}
	}

	public void loadUserTagData(String dbName) throws Exception {
		MongoDatabase db = mongoClient.getDatabase(dbName);

		MongoCollection coll = db.getCollection("users");
		// clear all data in the collection

		coll.deleteMany(new Document());

		Document doc;
		// Document doc1;

		// StringBuffer sb1 = new StringBuffer("");
		List<Integer> userListOfTag = new ArrayList<Integer>();

		List<Integer> userArtistTagList = new ArrayList<Integer>();
		List<Integer> userTagList = new ArrayList<Integer>();
		List<String> tagStampList = new ArrayList<String>();
		int temptag = 2;

		FileReader tagsreader = new FileReader("D://comp5338Project//project//user_taggedartists-timestamps.dat");
		BufferedReader bruser = new BufferedReader(tagsreader);

		String str = null;
		String[] taguserarr = null;
		int count = 0;
		while ((str = bruser.readLine()) != null) {
			if (count == 0) {
				count++;
				continue;
			}

			taguserarr = str.split("\t");
			userListOfTag.add(Integer.parseInt(taguserarr[0]));
			userArtistTagList.add(Integer.parseInt(taguserarr[1]));

			userTagList.add(Integer.parseInt(taguserarr[2]));

			tagStampList.add(taguserarr[3]);

		}

		List<Integer> singleUserList = new ArrayList<Integer>();
		int count4 = 0;
		for (int i = 0; i < userListOfTag.size(); i++) {

			if (temptag == userListOfTag.get(i)) {

				// friendList.add(userList.get(i));
				temptag = userListOfTag.get(i);

				if (i == (userListOfTag.size() - 1)) {
					count4++;
					singleUserList.add(count4);
				} else {
					count4++;
				}

			} else {
				singleUserList.add(count4);
				// count1 = 1;
				temptag = userListOfTag.get(i);
				count4++;
				if (i == (userListOfTag.size() - 1)) {
					singleUserList.add(count4);
				}

			}
		}

		HashSet h = new HashSet(userListOfTag);
		userListOfTag.clear();
		userListOfTag.addAll(h);
		Collections.sort(userListOfTag);
		List<Integer> subArtUserList = new ArrayList<Integer>();
		List<Integer> subTagUserList = new ArrayList<Integer>();
		List<String> subTagStampList = new ArrayList<String>();
		int tmpuserarttag = 0;
		System.out.println("friendList.size()" + singleUserList.size());
		for (int i = 0; i < singleUserList.size(); i++) {
			subArtUserList = userArtistTagList.subList(tmpuserarttag, singleUserList.get(i));
			subTagUserList = userTagList.subList(tmpuserarttag, singleUserList.get(i));
			subTagStampList = tagStampList.subList(tmpuserarttag, singleUserList.get(i));

			tmpuserarttag = singleUserList.get(i);
			System.out.println("subFriendList" + subArtUserList.size());

			List<Document> list_art_tag = new ArrayList<Document>();
			for (int j = 0; j < subArtUserList.size(); j++) {

				doc = new Document().append("mediaid", subArtUserList.get(j)).append("mediatag", subTagUserList.get(j))
						.append("tag_timestamp", subTagStampList.get(j));
				list_art_tag.add(doc);
				System.out.println("subFriendList.get(j)" + subArtUserList.get(j));

			}

			doc = new Document().append("id", userListOfTag.get(i)).append("mediaTags", list_art_tag);
			coll.insertOne(doc);
		}
	}

	public void loadMediaUserData(String dbName) throws Exception {
		MongoDatabase db = mongoClient.getDatabase(dbName);

		MongoCollection coll = db.getCollection("users");
		coll.createIndex(new Document("id", 1));
		// clear all data in the collection

		coll.deleteMany(new Document());

		Document doc = null;
		// Document doc1;
		//////////////////////////////////////////////////////////////

		List<Integer> userListOfTag = new ArrayList<Integer>();

		List<Integer> userArtistTagList = new ArrayList<Integer>();
		List<Integer> userTagList = new ArrayList<Integer>();
		List<String> tagStampList = new ArrayList<String>();
		int temptag = 2;

		// FileReader tagsreader = new
		// FileReader("D://comp5338Project//project//user_taggedartists-timestamps.dat");
		// BufferedReader brtag = new BufferedReader(tagsreader);

		// String strtag = null;
		// String[] taguserarr = null;
		// int count_tag = 0;
		// Date date = new Date();
		// while ((strtag = brtag.readLine()) != null) {
		// if (count_tag == 0) {
		// count_tag++;
		// continue;
		// }
		//
		// taguserarr = strtag.split("\t");
		// userListOfTag.add(Integer.parseInt(taguserarr[0]));
		// userArtistTagList.add(Integer.parseInt(taguserarr[1]));
		//
		// userTagList.add(Integer.parseInt(taguserarr[2]));
		//
		// // Long timestamp = Long.parseLong(taguserarr[3]);
		// // String dateStr = new
		// // java.text.SimpleDateFormat("yyyy-MM-dd").format(new
		// // java.util.Date(timestamp));
		// // DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// // date = sdf.parse(dateStr);
		// tagStampList.add(taguserarr[3]);
		//
		// }
		//
		// List<Integer> singleUserList = new ArrayList<Integer>();
		// int count4 = 0;
		// for (int i = 0; i < userListOfTag.size(); i++) {
		//
		// if (temptag == userListOfTag.get(i)) {
		//
		// // friendList.add(userList.get(i));
		// temptag = userListOfTag.get(i);
		//
		// if (i == (userListOfTag.size() - 1)) {
		// count4++;
		// singleUserList.add(count4);
		// } else {
		// count4++;
		// }
		//
		// } else {
		// singleUserList.add(count4);
		// // count1 = 1;
		// temptag = userListOfTag.get(i);
		// count4++;
		// if (i == (userListOfTag.size() - 1)) {
		// singleUserList.add(count4);
		// }
		//
		// }
		// }

		//////////////////////////////////////////////////////////////

		List<Integer> userListOfFriend = new ArrayList<Integer>();

		List<Integer> userFriendList = new ArrayList<Integer>();
		int tempuser = 2;

		FileReader userreader = new FileReader("D://comp5338Project//project//user_friends.dat");
		BufferedReader bruser = new BufferedReader(userreader);

		String str = null;
		String[] userarr = null;
		int count = 0;
		while ((str = bruser.readLine()) != null) {
			if (count == 0) {
				count++;
				continue;
			}

			userarr = str.split("\t");
			userListOfFriend.add(Integer.parseInt(userarr[0]));
			userFriendList.add(Integer.parseInt(userarr[1]));

		}

		List<Integer> friendList = new ArrayList<Integer>();
		int count3 = 0;
		for (int i = 0; i < userListOfFriend.size(); i++) {

			if (tempuser == userListOfFriend.get(i)) {

				// friendList.add(userList.get(i));
				tempuser = userListOfFriend.get(i);

				if (i == (userListOfFriend.size() - 1)) {
					count3++;
					friendList.add(count3);
				} else {
					count3++;
				}

			} else {
				friendList.add(count3);
				// count1 = 1;
				tempuser = userListOfFriend.get(i);
				count3++;
				if (i == (userListOfFriend.size() - 1)) {
					friendList.add(count3);
				}

			}
		}

		StringBuffer sb = new StringBuffer("");
		List<Integer> userList = new ArrayList<Integer>();

		List<Integer> userMediaList = new ArrayList<Integer>();
		List<Integer> mediaWeightList = new ArrayList<Integer>();
		int temp = 2;

		FileReader reader = new FileReader("D://comp5338Project//project//user_artists.dat");
		BufferedReader br = new BufferedReader(reader);

		String strmu = null;
		String[] muarr = null;
		int count_mu = 0;
		while ((strmu = br.readLine()) != null) {
			if (count_mu == 0) {
				count_mu++;
				continue;
			}

			muarr = strmu.split("\t");
			userList.add(Integer.parseInt(muarr[0]));
			userMediaList.add(Integer.parseInt(muarr[1]));
			mediaWeightList.add(Integer.parseInt(muarr[2]));

		}

		List<Integer> umList = new ArrayList<Integer>();
		int count1 = 0;
		for (int i = 0; i < userList.size(); i++) {

			if (temp == userList.get(i)) {

				// friendList.add(userList.get(i));
				temp = userList.get(i);

				if (i == (userList.size() - 1)) {
					count1++;
					umList.add(count1);
				} else {
					count1++;
				}

			} else {
				umList.add(count1);
				// count1 = 1;
				temp = userList.get(i);
				count1++;
				if (i == (userList.size() - 1)) {
					umList.add(count1);
				}

			}
		}

		HashSet h = new HashSet(userList);
		userList.clear();
		userList.addAll(h);
		Collections.sort(userList);
		// for(int m = 0; m<umList.size();m++){
		// System.out.println(userList.get(m));
		// System.out.println(umList.get(m));
		// }

		List<Integer> subMediaList = new ArrayList<Integer>();
		List<Integer> subWeightList = new ArrayList<Integer>();

		List<Integer> subFriendList = new ArrayList<Integer>();

		// List<Integer> subArtUserList = new ArrayList<Integer>();
		// List<Integer> subTagUserList = new ArrayList<Integer>();
		// List<String> subTagStampList = new ArrayList<String>();
		int tmpuser = 0;

		int tmp = 0;

		int tmpuserarttag = 0;
		System.out.println("umList.size()" + umList.size());
		for (int i = 0; i < umList.size(); i++) {
			subMediaList = userMediaList.subList(tmp, umList.get(i));
			subWeightList = mediaWeightList.subList(tmp, umList.get(i));
			tmp = umList.get(i);
			// System.out.println("subFriendList" + subMediaList.size());

			/////
			subFriendList = userFriendList.subList(tmpuser, friendList.get(i));
			tmpuser = friendList.get(i);
			/////

			////
			// subArtUserList = userArtistTagList.subList(tmpuserarttag,
			// singleUserList.get(i));
			// subTagUserList = userTagList.subList(tmpuserarttag,
			// singleUserList.get(i));
			// subTagStampList = tagStampList.subList(tmpuserarttag,
			// singleUserList.get(i));
			//
			// tmpuserarttag = singleUserList.get(i);
			////

			List<Document> listdoc = new ArrayList<Document>();

			List<Integer> testf = new ArrayList<Integer>();
			List<Document> list_art_tag = new ArrayList<Document>();

			for (int j = 0; j < subMediaList.size(); j++) {
				doc = new Document().append("mediaid", subMediaList.get(j)).append("mediaweight", subWeightList.get(j));

				// doc.put("mediaid", subMediaList.get(j));
				// doc.put("mediaweight", subWeightList.get(j));
				// mapmw.put(subMediaList.get(j), subWeightList.get(j));
				listdoc.add(doc);
				// testf.add(subMediaList.get(j));
				// testw.add(subWeightList.get(j));
				// System.out.println("subFriendList.get(j)" +
				// subMediaList.get(j));

			}
			for (int j = 0; j < subFriendList.size(); j++) {
				///
				testf.add(subFriendList.get(j));
				///
			}

			// for (int j = 0; j < subArtUserList.size(); j++) {
			//
			// doc = new Document().append("mediaid",
			// subArtUserList.get(j)).append("mediatag", subTagUserList.get(j))
			// .append("tag_timestamp", subTagStampList.get(j));
			// list_art_tag.add(doc);
			// // System.out.println("subFriendList.get(j)" +
			// // subArtUserList.get(j));
			//
			// }
			doc = new Document().append("id", userList.get(i)).append("artist", listdoc).append("friends", testf); // .append("art_tags",
																													// list_art_tag)

			coll.insertOne(doc);
			// coll.updateMany(Filters.eq("id", userList.get(i)),new
			// Document("$set",doc));

		}
	}

}
