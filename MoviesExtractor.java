package moviesExtractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;

public class MoviesExtractor {

	private static final String GET_MOVIE_API_URL = "https://api.themoviedb.org/3/movie/";
	private static final String API_KEY = "37662c76ffc19e5cd1b95f37d77155fc";
	private static final String LANGUAGE = "ru-RU";

	/**
	 * Executes requests in loop one by one for getting data about movie.
	 * Process it and write in .tsv file.
	 * 
	 * @param startIndex - Index from which loop starts to work.
	 * @param endIndex - Index to which loop starts to work.
	 */
	public void populateMovies(int startIndex, int endIndex)
			throws IOException, JSONException, InterruptedException {
		
		BufferedWriter tsv_bwr = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("tmdb_movies_" + startIndex + "_" + endIndex + ".tsv"), "Cp1251"));
		
		TsvWriter writer = new TsvWriter(tsv_bwr, new TsvWriterSettings());
		
		writer.writeHeaders("Id", "Format", "Imdb_id", "Vote_average", "Release_date", "Genres", "Original_title",
				"Title", "Overview", "Backdrop_path", "Poster_path", "Director");

		for (int i = startIndex; i < endIndex; i++) {
			String request_url = GET_MOVIE_API_URL + i + "?api_key=" + API_KEY + "&language=" + LANGUAGE
					+ "&append_to_response=credits";

			URL url_object = new URL(request_url);
			HttpURLConnection con = (HttpURLConnection) url_object.openConnection();
			con.setRequestMethod("GET");

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'GET' request for movie with id " + i + " to URL : " + request_url);
			System.out.println("Response Code : " + responseCode);

			// Timeout needs for workaround the limit on requests number per minute (from one ip address).
			TimeUnit.MILLISECONDS.sleep(100);

			if (responseCode != 404) {
				BufferedReader input = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
				String inputLine;
				StringBuffer response = new StringBuffer();

				while ((inputLine = input.readLine()) != null) {
					response.append(inputLine);
				}

				JSONObject json_response = new JSONObject(response.toString());

				// Extract properties from JSON response.
				int id = json_response.getInt("id");
				String format = "Movie";
				String imdb_id = json_response.getString("imdb_id");
				double vote_average = json_response.getDouble("vote_average");
				String release_date = json_response.getString("release_date");
				String genres = "";
				JSONArray genres_array = json_response.getJSONArray("genres");
				for (int j = 0; j < genres_array.length(); j++) {
					genres += genres_array.getJSONObject(j).getInt("id");
					if (j != genres_array.length() - 1) {
						genres += ",";
					}
				}
				String original_title = json_response.getString("original_title");
				String title = json_response.getString("title");
				String overview = json_response.getString("overview");
				String backdrop_path = json_response.isNull("backdrop_path") ? "null"
						: json_response.getString("backdrop_path");
				String poster_path = json_response.isNull("poster_path") ? "null"
						: json_response.getString("poster_path");
				String director = "";
				JSONArray crew_array = json_response.getJSONObject("credits").getJSONArray("crew");
				for (int x = 0; x < crew_array.length(); x++) {
					if (crew_array.getJSONObject(x).getString("job").equals("Director")) {
						director = crew_array.getJSONObject(x).getString("name");
						break;
					}
				}
				
				// Write row in .tsv file using TSV writer.
				writer.writeRow(new Object[] { id, format, imdb_id, vote_average, release_date, genres, original_title,
						title, overview, backdrop_path, poster_path, director });
				
				writer.flush();
				input.close();
			}
		}
		writer.close();
	}

	/**
	 * Print in console message about program execution time.
	 * 
	 * @param startDate Start time of program.
	 * @param endDate End time of program.
	 */
	public void printDifference(Date startDate, Date endDate){

		long different = endDate.getTime() - startDate.getTime();

		System.out.println("\n");
		System.out.println("startDate : " + startDate);
		System.out.println("endDate : "+ endDate);

		long secondsInMilli = 1000;
		long minutesInMilli = secondsInMilli * 60;
		long hoursInMilli = minutesInMilli * 60;
		long daysInMilli = hoursInMilli * 24;

		long elapsedDays = different / daysInMilli;
		different = different % daysInMilli;

		long elapsedHours = different / hoursInMilli;
		different = different % hoursInMilli;

		long elapsedMinutes = different / minutesInMilli;
		different = different % minutesInMilli;

		long elapsedSeconds = different / secondsInMilli;

		System.out.printf(
		    "%d days, %d hours, %d minutes, %d seconds%n",
		    elapsedDays,
		    elapsedHours, elapsedMinutes, elapsedSeconds);
	}

	public static void main(String[] args) throws IOException, JSONException, InterruptedException {
		
		MoviesExtractor moviesExtractor = new MoviesExtractor();

		Date start = new Date();
		
		moviesExtractor.populateMovies(110000, 120000);
		
		Date end = new Date();
		
		moviesExtractor.printDifference(start, end);		
		
	}

}
