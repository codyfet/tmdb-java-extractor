package moviesExtractor;

import java.io.*;
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

    int startIndex;
    int endIndex;
    File resultFile;
    TsvWriter writer;

    /**
     * @param startIndex
     * @param endIndex
     */
    public MoviesExtractor(int startIndex, int endIndex) throws FileNotFoundException, UnsupportedEncodingException {
        File file = new File("tmdb_movies_" + startIndex + "_" + endIndex + ".tsv");
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.resultFile = file;

        BufferedWriter tsv_bwr = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file)));

        this.writer = new TsvWriter(tsv_bwr, new TsvWriterSettings());

        this.writer.writeHeaders("Id", "Format", "Imdb_id", "Vote_average", "Release_date", "Genres", "Original_title",
                "Title", "Overview", "Backdrop_path", "Poster_path", "Director");
    }

    /**
     * Print in console message about program execution time.
     *
     * @param startDate Start time of program.
     * @param endDate   End time of program.
     */
    private void printDifference(Date startDate, Date endDate) {

        long different = endDate.getTime() - startDate.getTime();

        System.out.println("\n");
        System.out.println("startDate : " + startDate);
        System.out.println("endDate : " + endDate);

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

    /**
     * Executes TMDB getMovie request.
     *
     * @param index TMDB ID of requested movie.
     * @throws IOException
     */
    private String executeRequest(int index) throws Exception {
        String request_url;
        URL url_object;
        HttpURLConnection con;
        int responseCode;
        String responseString = null;

        request_url = GET_MOVIE_API_URL + index + "?api_key=" + API_KEY + "&language=" + LANGUAGE
                + "&append_to_response=credits";

        url_object = new URL(request_url);

        con = (HttpURLConnection) url_object.openConnection();
        con.setRequestMethod("GET");
        System.out.println("\nSending 'GET' request for movie with id " + index + " to URL : " + request_url);

        responseCode = con.getResponseCode();
        System.out.println("Response Code : " + responseCode);

        if (responseCode != 404) {
            BufferedReader input = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = input.readLine()) != null) {
                response.append(inputLine);
            }
            input.close();
            responseString = response.toString();
        }
        return responseString;
    }

    /**
     * @param response String with response from TMDB getMovie service.
     * @throws JSONException
     */
    private Object[] extractProperties(String response) throws JSONException {
        JSONObject json_response = new JSONObject(response);

        // Extract properties from JSON response.
        int id = json_response.getInt("id");
        String format = "Movie";
        String imdb_id = json_response.isNull("imdb_id") ? null : json_response.getString("imdb_id");
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

        return new Object[]{id, format, imdb_id, vote_average, release_date, genres, original_title,
                title, overview, backdrop_path, poster_path, director};
    }

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, UnsupportedEncodingException {

        // Put in constructor startIndex and endIndex of range movies you want to extract.
        MoviesExtractor moviesExtractor = new MoviesExtractor(0, 1000);

        Date start = new Date();

        try {

            for (int i = moviesExtractor.startIndex; i < moviesExtractor.endIndex; i++) {
                String response = moviesExtractor.executeRequest(i);
                if (response != null) {
                    Object[] row = moviesExtractor.extractProperties(response);
                    moviesExtractor.writer.writeRow(row);
                    moviesExtractor.writer.flush();
                }
                // Timeout needs for workaround the limit on requests number per minute (from one ip address).
                TimeUnit.MILLISECONDS.sleep(100);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            moviesExtractor.writer.close();
        }

        Date end = new Date();

        moviesExtractor.printDifference(start, end);

    }

}
