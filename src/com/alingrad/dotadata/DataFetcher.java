package com.alingrad.dotadata;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DataFetcher {
	
	public static final String API_KEY = "1A71970C0A3D1FC66A2CCC97CDD957C0";
	public static final String MATCH_HISTORY_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001";
	public static final String MATCH_DETAILS_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001";
	
	public static final String[] MATCH_FIELDS = new String[] {"match_id", "game_mode", "radiant_win", "duration"};
	public static final String[] PLAYER_FIELDS = new String[] {"player_slot", "hero_id", "item_0", "item_1", "item_2", "item_3", "item_4", "item_5", "kills", "deaths", "assists", "last_hits", "denies", "gold_per_min", "xp_per_min", "hero_damage", "tower_damage", "hero_healing"};
	
	private HttpClient mClient;
	
	private JSONParser mParser;
	private PrintWriter mOut;
	private int count;
	private long fetch_count;
	private long num_requests;
	private long startMs;
	private Map<Long, Boolean> seenMatches;
	
	public DataFetcher(String outputFileName) throws IOException {
		mClient = new DefaultHttpClient();
		mParser = new JSONParser();
		mOut = new PrintWriter(new FileWriter(outputFileName));
		count = 0;
		mOut.println("[");
		seenMatches = new HashMap<Long, Boolean>();
		fetch_count = 0;
		num_requests = 0;
		startMs = System.currentTimeMillis();
	}
	
	public String fetchMatch(long match_id) throws URISyntaxException, ClientProtocolException, IOException {
		URIBuilder builder = new URIBuilder();
		builder.setPath(MATCH_DETAILS_URL)
		    .setParameter("key", API_KEY)
		    .setParameter("match_id", "" + match_id);
		
		URI uri = builder.build();
		HttpGet get = new HttpGet(uri);
		HttpResponse response = mClient.execute(get);
		num_requests++;
		HttpEntity entity = response.getEntity();
		InputStream inputStream = entity.getContent();

	    ByteArrayOutputStream content = new ByteArrayOutputStream();

	    // Read response into a buffered stream
	    int readBytes = 0;
	    byte[] sBuffer = new byte[512];
	    while ((readBytes = inputStream.read(sBuffer)) != -1) {
	        content.write(sBuffer, 0, readBytes);
	    }

	    // Return result from buffered stream
	   return new String(content.toByteArray());
	}
	
	public String fetchMatchHistory() throws ClientProtocolException, URISyntaxException, IOException {
		return fetchMatchHistory(-1);
	}
	
	public String fetchMatchHistory(long startFrom) throws URISyntaxException, ClientProtocolException, IOException {
		URIBuilder builder = new URIBuilder();
		builder.setPath(MATCH_HISTORY_URL)
		    .setParameter("key", API_KEY)
		    .setParameter("min_players", "" + 10);
		if (startFrom != -1)
			builder.setParameter("start_at_match_id", "" + startFrom);
		URI uri = builder.build();
		HttpGet get = new HttpGet(uri);
		HttpResponse response = mClient.execute(get);
		num_requests++;
		HttpEntity entity = response.getEntity();
		InputStream inputStream = entity.getContent();

	    ByteArrayOutputStream content = new ByteArrayOutputStream();

	    // Read response into a buffered stream
	    int readBytes = 0;
	    byte[] sBuffer = new byte[512];
	    while ((readBytes = inputStream.read(sBuffer)) != -1) {
	        content.write(sBuffer, 0, readBytes);
	    }

	    // Return result from buffered stream
	   return new String(content.toByteArray());
	}
	
	public void fetchAllLatestMatches() throws ClientProtocolException, URISyntaxException, IOException {
		long from_match_id = -1;
		while (true) {
			String data = fetchMatchHistory(from_match_id);
			fetch_count++;
			try {
				JSONObject obj = (JSONObject)mParser.parse(data);
				JSONObject result = (JSONObject)obj.get("result");
				if (((Long)result.get("status")) == 1) {
					long min_match_id = -1;
					long num_results = (Long)result.get("num_results");
					if (num_results > 0) {
						JSONArray matches = (JSONArray)result.get("matches");
						for (int i = 0; i < num_results; i++) {
							JSONObject match = (JSONObject)matches.get(i);
							long match_id = (Long)match.get("match_id");
							if (!seenMatches.containsKey(match_id)) {
								if (min_match_id == -1 || match_id < min_match_id)
									min_match_id = match_id;
								
								try {
									JSONObject match_details = (JSONObject)mParser.parse(fetchMatch(match_id));
									JSONObject match_details_result = (JSONObject)match_details.get("result");
									if (match_details_result != null) {
										String str = match_details_result.toString();
										if (str != null && !str.equals("")) {
											long lobbyType = (Long)(match_details_result.get("lobby_type"));
											long game_mode = (Long)(match_details_result.get("game_mode"));
											long duration = (Long)(match_details_result.get("duration"));
											if (lobbyType == 0 && (game_mode == 0 || game_mode == 1 || game_mode == 4 || game_mode == 5) && duration >= 600) {
												System.out.println(count + ": " + match_id + ", game_mode: " + game_mode + ", duration: " + duration);
												
												if (count != 0)
													mOut.print(",");
												
												mOut.print("{");
												for (int k = 0; k < MATCH_FIELDS.length; k++) {
													String field = MATCH_FIELDS[k];
													if (k != 0)
														mOut.print(",");
													mOut.print("\"" + field + "\":" + match_details_result.get(field));
												}
												JSONArray players = (JSONArray)(match_details_result.get("players"));
												mOut.print(",\"players\":[");
												for (int j = 0; j < players.size(); j++) {
													JSONObject player = (JSONObject)players.get(j);
													if (j != 0)
														mOut.print(",");
													mOut.print("{");
													for (int k = 0; k < PLAYER_FIELDS.length; k++) {
														String field = PLAYER_FIELDS[k];
														if (k != 0)
															mOut.print(",");
														mOut.print("\"" + field + "\":" + player.get(field));
													}
													mOut.print("}");
												}
												mOut.print("]");
												mOut.print("}");
												mOut.println();
												seenMatches.put(match_id, true);
												count++;
											} else {
												System.out.println("N/A: " + match_id + ", game_mode: " + game_mode + ", duration: " + duration);
											}
										}
									}
								} catch(Exception ex) {
									System.out.print("Parse Match Step: ");
									ex.printStackTrace();
								}
							}
						}
						from_match_id = min_match_id - 1;
					} else {
						break;
					}
				}
			} catch (ParseException ex) {
				ex.printStackTrace();
			}
		}
		long ms = System.currentTimeMillis();
		System.out.println("FETCHED: " + fetch_count + ", requests per second: " + (num_requests / ((ms - startMs) / 1000f)));
	}

	public void close() {
		mOut.println("]");
		mOut.close();
	}
	
	public static void main(String[] args) {
		DataFetcher fetcher = null;
		try {
			fetcher = new DataFetcher("dota_data.json");
		} catch (Exception ex) {
			
		}
		if (fetcher != null) {
			while (true) {
				try {
					fetcher.fetchAllLatestMatches();
					if (fetcher.getCount() > 10000)
						break;
				} catch(Exception ex) {
					ex.printStackTrace();
				}
				try {
					Thread.sleep(60000);
				} catch(InterruptedException ex) {
				}
			}
		}
		fetcher.close();
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
}
