package com.neo4j.query;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;

public class Neo4JSimpleQuery {

	/**
	 * Smimple query one given a user id, find all artists the user’s friends
	 * listen.
	 * 
	 * match (u1:Users{name:9})-[f:FRIENDS]-(u2:Users) with u2 match
	 * (u2)-[:LISTEN]-(music:MusicArtists) return music,u2
	 */

	/**
	 * Simple query two given an artist name, find the most recent 10 tags that
	 * have been assigned to it.
	 * 
	 * match (a:MusicArtists {name:'MALICE MIZER'})-[t:TAG]-() with t order by
	 * t.tagtimestam desc limit 10 unwind t.tagid as tid match (ta:Tags) where
	 * ta.tagid=tid return ta
	 * 
	 * @param session
	 * @param artist_name
	 * @throws ParseException
	 */
	public static void simpleQuerySecond(Session session, String artist_name) throws ParseException {
		StatementResult tag_list = session.run("match (a:MusicArtists {name:'" + artist_name
				+ "'})-[t:TAG]-() return t.tagtimestam as tagtimestam, t.tagid as tagid");

		String timestamp = null;
		Integer tagid = 0;

		List<Map<Date, Integer>> tag_time_list = new ArrayList<Map<Date, Integer>>();

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date date = new Date();
		while (tag_list.hasNext()) {
			Map<Date, Integer> map = new HashMap<Date, Integer>(); // timestamp
																	// as key,
																	// tagid as
																	// value

			Record tag_detail = tag_list.next();
			timestamp = tag_detail.get("tagtimestam").asString();
			String dateStr = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
					.format(new java.util.Date(Long.parseLong(timestamp)));
			date = sdf.parse(dateStr);

			tagid = tag_detail.get("tagid").asInt();

			map.put(date, tagid);
			tag_time_list.add(map);
			// System.out.println(date+":"+tagid);
		}

		Comparator<Map<Date, Integer>> comparator = new Comparator<Map<Date, Integer>>() {
			public int compare(Map<Date, Integer> o1, Map<Date, Integer> o2) {
				Date o1_1 = new Date();
				Date o2_2 = new Date();
				for (Entry<Date, Integer> e : o1.entrySet()) {
					o1_1 = e.getKey();
					break;
				}
				for (Entry<Date, Integer> e : o2.entrySet()) {
					o2_2 = e.getKey();
					break;
				}
				return o1_1.compareTo(o2_2);
			}
		};

		Collections.sort(tag_time_list, comparator);

		for (int i = tag_time_list.size() - 1; i >= tag_time_list.size() - 10; i--) {
			System.out.print(tag_time_list.get(i));
			String tid = tag_time_list.get(i).values().toString();
			String tid1 = tid.replace("[", "");
			String tid2 = tid1.replace("]", "");
			StatementResult tags_value = session.run("match (t:Tags {tagid:" + Integer.parseInt(tid2)
					+ "}) return t.tagvalue as tagvalue, t.tagid as tagid");
			while (tags_value.hasNext()) {
				Record tag = tags_value.next();
				String tv = tag.get("tagvalue").asString();
				System.out.print(tv);
			}
			System.out.println();
		}
	}
	/**
	 * given an artist name, find the top 10 users based on their respective
	 * listening counts of this artist. Display both the user id and the
	 * listening count
	 * 
	 * match (a:MusicArtists {name:"Behemoth"})-[l:LISTEN]-(u:Users) return
	 * u.name,l.weight order by l.weight DESC LIMIT 10
	 * 
	 */
	/**
	 * given a user id, find the most recent 10 artists the user has assigned
	 * tag to.
	 * 
	 * MATCH (u:Users{name:3})-[r:TAG]->() RETURN r order by r.tagtimestam DESC
	 * LIMIT 10
	 */

}
