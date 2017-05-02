package com.neo4j.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Neo4JComplexQuery {

	/**
	 * find the top 5 artists ranked by the number of users listening to it
	 * 
	 * match (a:MusicArtists)-[l:LISTEN]-(u:Users) with a,count(u) as ucount
	 * order by ucount desc limit 5 return a,ucount
	 * 
	 * @param session
	 * @throws Exception
	 */
	public static void complexQueryFirst(Session session) throws Exception {

		StatementResult artist_list = session.run("match (a:MusicArtists) return a.artistid as artistid");
		Map<Integer, Integer> artist_listen_map = new HashMap<Integer, Integer>();
		while (artist_list.hasNext()) {
			Record artist = artist_list.next();
			Integer artist_id = artist.get("artistid").asInt();
			Integer l_count = 0;
			// find each artist's listeners total number (total number of LISTEN
			// relationships)
			StatementResult artist_detail = session.run("MATCH (a:MusicArtists{artistid:" + artist_id
					+ "})-[l:LISTEN]-(u:Users) RETURN count(l) as usersnum");
			while (artist_detail.hasNext()) {
				Record ad = artist_detail.next();
				l_count = ad.get("usersnum").asInt();
			}
			artist_listen_map.put(artist_id, l_count);
		}

		List<Map.Entry<Integer, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<Integer, Integer>>(artist_listen_map.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> mapping1, Map.Entry<Integer, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});

		List<Integer> topArtists = new ArrayList<Integer>();
		for (Map.Entry<Integer, Integer> mapping : mappingList) {

			topArtists.add(mapping.getKey());
		}
		Integer aid = 0;
		String url = null;
		String pu = null;
		String aname = null;
		Integer count_top = 0;
		for (int i = 0; i < 5; i++) {
			StatementResult top_artist = session.run("MATCH (a:MusicArtists{artistid:" + topArtists.get(i)
					+ "})-[l:LISTEN]-(u:Users) RETURN a.name as name, a.artistid as artistid, a.url as url, a.pictureURL as pu");
			while (top_artist.hasNext()) {
				Record top = top_artist.next();
				aid = top.get("artistid").asInt();
				aname = top.get("name").asString();
				url = top.get("url").asString();
				pu = top.get("pu").asString();

			}
			StatementResult count_user = session.run("MATCH (a:MusicArtists{artistid:" + topArtists.get(i)
					+ "})-[l:LISTEN]-(u:Users) return count(l) as count");
			while (count_user.hasNext()) {
				Record top_count = count_user.next();
				count_top = top_count.get("count").asInt();
			}
			System.out.println("ArtistId:" + aid + " --- " + "ArtistName:" + aname + " --- " + "URL:" + url
					+ " --- pictureURL:" + pu + " --- TotalUsers:" + count_top);

		}

	}

	/**
	 * given an artist name, find the top 20 tags assigned to it. The tags are
	 * ranked by the number of times it has been assigned to this artist
	 * 
	 * @param session
	 * @param artist_name
	 * @throws Exception
	 */
	public static void complexQuerySecond(Session session, String artist_name) throws Exception {
		StatementResult artist_tag_id = session
				.run("MATCH (a:MusicArtists{name:'" + artist_name + "'})-[t:TAG]-(u:Users) return t.tagid as tagid");

		Integer tag_id = 0;
		int value = 0;
		int tmp = 0;
		Map<Integer, Integer> map_tag_id_count = new HashMap<Integer, Integer>();
		while (artist_tag_id.hasNext()) {
			Record tagid = artist_tag_id.next();
			tag_id = tagid.get("tagid").asInt();
			if (map_tag_id_count.get(tag_id) == null) {
				value = 1;
				map_tag_id_count.put(tag_id, value);
			} else {
				tmp = map_tag_id_count.get(tag_id);
				map_tag_id_count.put(tag_id, tmp + 1);
			}
		}

		List<Map.Entry<Integer, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<Integer, Integer>>(map_tag_id_count.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> mapping1, Map.Entry<Integer, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});

		if (mappingList.size() > 20) {
			String tagvaules = null;
			for (int i = 0; i < 20; i++) {
				Map.Entry<Integer, Integer> mapping = mappingList.get(i);

				StatementResult tag_value_result = session
						.run("match (t:Tags{tagid:" + mapping.getKey() + "}) return t.tagvalue as value");
				while (tag_value_result.hasNext()) {
					Record tagv = tag_value_result.next();
					tagvaules = tagv.get("value").asString();
				}

				System.out.println("TagId:" + mapping.getKey() + " --- TagValue:" + tagvaules + " --- Count:"
						+ mapping.getValue());

			}
		} else {
			String tagvaules = null;
			for (int i = 0; i < mappingList.size(); i++) {
				Map.Entry<Integer, Integer> mapping = mappingList.get(i);

				StatementResult tag_value_result = session
						.run("match (t:Tags{tagid:" + mapping.getKey() + "}) return t.tagvalue as value");
				while (tag_value_result.hasNext()) {
					Record tagv = tag_value_result.next();
					tagvaules = tagv.get("value").asString();
				}

				System.out.println("TagId:" + mapping.getKey() + " --- TagValue:" + tagvaules + " --- Count:"
						+ mapping.getValue());
			}
		}
	}

	/**
	 * given a user id, find the top 5 artists listened by his friends but not
	 * him. We rank artists by the sum of friends’ listening counts of the
	 * artist.
	 * 
	 * @param session
	 * @param user_id
	 * @throws Exception
	 */
	public static void complexQueryThird(Session session, int user_id) throws Exception {
		StatementResult user_artist_list = session.run(
				"match (u:Users{name:" + user_id + "})-[l:LISTEN]->(a:MusicArtists) return a.artistid as artistid");
		List<Integer> user_art_id = new ArrayList<Integer>();
		Integer user_artid;
		while (user_artist_list.hasNext()) {
			Record user_artist = user_artist_list.next();
			user_artid = user_artist.get("artistid").asInt();
			user_art_id.add(user_artid);

		}

		StatementResult friend_artist_list = session.run("match (u:Users{name:" + user_id
				+ "})-[f:FRIENDS]->(u2:Users) with u2 match (u2)-[l2:LISTEN]->(a2:MusicArtists) return a2.artistid as artistid,l2.weight as weight ");
		Integer friend_artistid = 0;
		Integer friend_weight = 0;

		Map<Integer, Integer> artist_weight_map = new HashMap<Integer, Integer>();
		int tmp = 0;
		int sum = 0;
		while (friend_artist_list.hasNext()) {

			Record friend_artist = friend_artist_list.next();
			friend_artistid = friend_artist.get("artistid").asInt();
			friend_weight = friend_artist.get("weight").asInt();
			if (artist_weight_map.get(friend_artistid) == null) {
				artist_weight_map.put(friend_artistid, friend_weight);
			} else {
				tmp = artist_weight_map.get(friend_artistid);
				sum = tmp + friend_weight;
				artist_weight_map.put(friend_artistid, sum);
			}
		}

		// remove the duplicate value
		Iterator<Integer> iter = artist_weight_map.keySet().iterator();
		while (iter.hasNext()) {
			int k = iter.next();
			for (int i = 0; i < user_art_id.size(); i++) {
				if (k == user_art_id.get(i)) {
					iter.remove();
					artist_weight_map.remove(k);

				}
			}
		}

		List<Map.Entry<Integer, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<Integer, Integer>>(artist_weight_map.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> mapping1, Map.Entry<Integer, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});

		for (int i = 0; i < 5; i++) {
			Map.Entry<Integer, Integer> mapping = mappingList.get(i);
			StatementResult art_result = session.run("match (a:MusicArtists{artistid:" + mapping.getKey()
					+ "}) return a.artistid as artistid, a.name as name,a.url as url, a.pictureURL as pu");
			while (art_result.hasNext()) {
				Record art = art_result.next();
				System.out.print("ArtistId:" + art.get("artistid").asInt() + " --- ArtistName:"
						+ art.get("name").asString() + " --- URL:" + art.get("url") + " --- picURL:" + art.get("pu"));
			}
			System.out.println(" --- TotalWeight" + mapping.getValue());
		}

	}

	/**
	 * given an artist name, find the top 5 similar artists. Here similarity
	 * between a pair of artists is defined by the number of unique users that
	 * have listened both. The higher the number, the more similar the two
	 * artists are.
	 * 
	 * @param session
	 * @param artist_name
	 * @throws Exception
	 */

	public static void complexQueryFourth(Session session, String artist_name) throws Exception {
		StatementResult user_list_per_artist = session.run("match (u:Users)-[l:LISTEN]->(a:MusicArtists{name:'"
				+ artist_name
				+ "'}) with u,a match (u)-[l1:LISTEN]->(a1:MusicArtists) return a1.artistid as artistid, a.artistid as orgartistid");
		List<Integer> total_artist_list = new ArrayList<Integer>();
		int artist_id = 0;
		while (user_list_per_artist.hasNext()) {
			Record userid_result = user_list_per_artist.next();
			total_artist_list.add(userid_result.get("artistid").asInt());
			artist_id = userid_result.get("orgartistid").asInt();
		}
		Map<Integer, Integer> artist_count_map = new HashMap<Integer, Integer>();
		int tmp = 0;
		for (int i = 0; i < total_artist_list.size(); i++) {
			if (artist_count_map.get(total_artist_list.get(i)) == null) {
				artist_count_map.put(total_artist_list.get(i), 1);
			} else {
				tmp = artist_count_map.get(total_artist_list.get(i));
				tmp++;
				artist_count_map.put(total_artist_list.get(i), tmp);
			}
		}

		List<Map.Entry<Integer, Integer>> mappingList = null;
		mappingList = new ArrayList<Map.Entry<Integer, Integer>>(artist_count_map.entrySet());

		Collections.sort(mappingList, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> mapping1, Map.Entry<Integer, Integer> mapping2) {
				return mapping2.getValue().compareTo(mapping1.getValue());
			}
		});

		for (int i = 0; i < 6; i++) {
			Map.Entry<Integer, Integer> mapping = mappingList.get(i);

			if (mapping.getKey() == artist_id) {
				continue;
			} else {
				StatementResult top_artist = session.run("match (a:MusicArtists{artistid:" + mapping.getKey()
						+ "}) return a.artistid as artistid, a.name as name, a.url as url, a.pictureURL as pu");
				while (top_artist.hasNext()) {
					Record art = top_artist.next();
					System.out.print("ArtistId:" + art.get("artistid").asInt() + " --- ArtistName:"
							+ art.get("name").asString() + " --- URL:" + art.get("url").asString() + " --- picURL:"
							+ art.get("pu").asString());
				}
				System.out.println(" --- Count:" + mapping.getValue());
			}

		}
	}

}
