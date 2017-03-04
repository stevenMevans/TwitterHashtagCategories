import twitter4j.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Collector {
    private final static int MAX_QUANTITY = 125000;
    private final static String OUTPUT = "src/data/tweets.json";

    private static TwitterStream twitterStream;

    /*
     * Opens the Twitter public stream and listens for incoming tweets.
     * Tweets are written to a file in JSON format, to be processed later.
     * Once the specified number of tweets has been reached, the listener
     * is removed and the stream is closed.
     */
    public static void main(String[] args) throws TwitterException, IOException {
        FileWriter fw = new FileWriter(OUTPUT, true);
        BufferedWriter bw = new BufferedWriter(fw);

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(new StatusListener() {
            int tweetCount = 0;
            @Override
            public void onStatus(Status status) {
                if (tweetCount < MAX_QUANTITY) {
                    PrintWriter out = new PrintWriter(bw);
                    out.println(TwitterObjectFactory.getRawJSON(status));
                    if (++tweetCount % 1000 == 0)
                        System.out.println(tweetCount);
                }
                else { // Collection finished, close stream.
                    try { bw.flush(); }
                    catch (IOException e) { e.printStackTrace(); }
                    System.out.println(tweetCount + " tweets saved. ");
                    Collector.stop();
                }
            }
            /* Unimplemented Interface Methods */
            @Override
            public void onException(Exception ex)
            {
                ex.printStackTrace();
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}
            @Override
            public void onStallWarning(StallWarning warning) {}
        });
        // Initiate stream, filtering for tweets in English only
        twitterStream.sample("en");
    }

    private static void stop() {
        twitterStream.clearListeners();
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }
}
