package com.neo4j.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class LoadData {
	public static Driver constructDriver() throws Exception {
		// tag::construct-driver[]
		Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "neo4j"));
		// end::construct-driver[]

		return driver;
	}

	public static Driver configuration() throws Exception {
		// tag::configuration[]
		Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "neo4j"),
				Config.build().withMaxSessions(10).toConfig());
		// end::configuration[]

		return driver;
	}

	public static void insertData(Session session) throws Exception {
		List<String> query_list = new ArrayList<String>(); //Store all query syns
		
		//Add artist data
	
		InputStreamReader reader_art = new InputStreamReader(
				new FileInputStream("D://comp5338Project//project//artists.dat"), "UTF-8");
		BufferedReader br_art = new BufferedReader(reader_art);
		String str_art = null;
		String[] artarr = null;
		int count_art = 0;
		while ((str_art = br_art.readLine()) != null) {
			if (count_art == 0) {
				count_art++;
				continue;
			}

			artarr = str_art.split("\t");

			if (artarr.length > 3) {
				String create_artist_name = "CREATE (a"+Integer.parseInt(artarr[0])+":MusicArtists {artistid:"+Integer.parseInt(artarr[0])+",name:'"+artarr[1].replace("'", "")+"',url:'"+artarr[2]+"',pictureURL:'"+artarr[3]+"'})";
				session.run(create_artist_name);			
			} else {
				
				String create_artist_name = "CREATE (a"+Integer.parseInt(artarr[0])+":MusicArtists {artistid:"+Integer.parseInt(artarr[0])+",name:'"+artarr[1].replace("'", "")+"',url:'"+artarr[2]+"'})";
				session.run(create_artist_name);
				
			}

			count_art++;
		}

		session.run("CREATE CONSTRAINT ON (cc:MusicArtists) ASSERT cc.artistid IS UNIQUE");
		
		/////////////////////////////////////////////
		
		List<Integer> userListOfTag = new ArrayList<Integer>();

		List<Integer> userArtistTagList = new ArrayList<Integer>();
		List<Integer> userTagList = new ArrayList<Integer>();
		List<String> tagStampList = new ArrayList<String>();
		int temptag = 2;

		FileReader tagsreader = new FileReader("D://comp5338Project//project//user_taggedartists-timestamps.dat");
		BufferedReader brtag = new BufferedReader(tagsreader);

		String strtag = null;
		String[] taguserarr = null;
		int count_tag = 0;
		Date date = new Date();
		while ((strtag = brtag.readLine()) != null) {
			if (count_tag == 0) {
				count_tag++;
				continue;
			}

			taguserarr = strtag.split("\t");
			userListOfTag.add(Integer.parseInt(taguserarr[0]));
			userArtistTagList.add(Integer.parseInt(taguserarr[1]));

			userTagList.add(Integer.parseInt(taguserarr[2]));

			tagStampList.add(taguserarr[3]);

		}

		List<Integer> singleUserList = new ArrayList<Integer>();
		int count4 = 0;
		for (int i = 0; i < userListOfTag.size(); i++) {

			if (temptag == userListOfTag.get(i)) {

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
		
		session.run("CREATE CONSTRAINT ON (cc:Users) ASSERT cc.name IS UNIQUE");
		
		List<Integer> subMediaList = new ArrayList<Integer>();
		List<Integer> subWeightList = new ArrayList<Integer>();

		List<Integer> subFriendList = new ArrayList<Integer>();

		List<Integer> subArtUserList = new ArrayList<Integer>();
		List<Integer> subTagUserList = new ArrayList<Integer>();
		List<String> subTagStampList = new ArrayList<String>();
		int tmpuser = 0;

		int tmp = 0;

		int tmpuserarttag = 0;
		System.out.println("umList.size()" + umList.size());
		for (int i = 0; i < umList.size(); i++) {
			subMediaList = userMediaList.subList(tmp, umList.get(i));
			subWeightList = mediaWeightList.subList(tmp, umList.get(i));
			tmp = umList.get(i);
			

			/////
			subFriendList = userFriendList.subList(tmpuser, friendList.get(i));
			tmpuser = friendList.get(i);
			/////

			////
			subArtUserList = userArtistTagList.subList(tmpuserarttag, singleUserList.get(i));
			subTagUserList = userTagList.subList(tmpuserarttag, singleUserList.get(i));
			subTagStampList = tagStampList.subList(tmpuserarttag, singleUserList.get(i));

			tmpuserarttag = singleUserList.get(i);
			////			

			for (int j = 0; j < subMediaList.size(); j++) {

				String create_user_media_rel ="match (u1:Users{name:"+userList.get(i)+"}), (a1:MusicArtists{artistid:"+subMediaList.get(j)+"}) create (u1)-[:LISTEN {weight:"+subWeightList.get(j)+"}]->(a1)";
				session.run(create_user_media_rel);
//				System.out.println(create_user_media_rel);
//				query_list.add(create_user_media_rel);

			}
			for (int j = 0; j < subFriendList.size(); j++) {
				String create_user_friends_rel ="match (u1:Users{name:"+userList.get(i)+"}), (u2:Users{name:"+subFriendList.get(j)+"}) create (u1)-[:FRIENDS]->(u2)";

				session.run(create_user_friends_rel);
//				System.out.println(create_user_friends_rel);
//				query_list.add(create_user_friends_rel);
			}
//
			for (int j = 0; j < subArtUserList.size(); j++) {
				String create_user_tag_artist_rel = "match (u1:Users{name:"+userList.get(i)+"}), (a1:MusicArtists{artistid:"+subArtUserList.get(j)+"}) create (u1)-[:TAG {tagid:"+subTagUserList.get(j)+",tagtimestam:'"+subTagStampList.get(j)+"'}]->(a1)";

//				System.out.println(create_user_tag_artist_rel);
				session.run(create_user_tag_artist_rel);


			}

		
		}

		for(int i = 0; i<query_list.size(); i++){
			System.out.println(query_list.get(i));
			
		}

	}
	
	public static void insertTagData (Session session) throws NumberFormatException, IOException {
		FileReader reader_tag = new FileReader("D://comp5338Project//project//tags.dat");
		BufferedReader br_tag = new BufferedReader(reader_tag);
		String str_tag = null;
		String[] artarr = null;
		int count_tag = 0;
		session.run("CREATE CONSTRAINT ON (cc:Tags) ASSERT cc.tagid IS UNIQUE");

		while ((str_tag = br_tag.readLine()) != null) {
			if (count_tag == 0) {
				count_tag++;
				continue;
			}

			artarr = str_tag.split("\t");
			String create_tag = "CREATE (t"+Integer.parseInt(artarr[0])+":Tags {tagid:"+Integer.parseInt(artarr[0])+",tagvalue:'"+artarr[1].replaceAll("'","")+"'})";
			session.run(create_tag);

			count_tag++;
		}
	}

}
