import twitter4j.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TweetSampleCollector
{
    private final static int FILE_QUANTITY = 15;
    private final static String FILE_NAMES = "src/data/tweets.json";

    private static TwitterStream twitterStream;

    public static void stop()
    {
        twitterStream.clearListeners();
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    public static void main(String[] args) throws TwitterException, IOException
    {
        FileWriter fw = new FileWriter(FILE_NAMES, true);
        BufferedWriter bw = new BufferedWriter(fw);

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(new StatusListener() {
            int tweetCount = 0;
            @Override
            public void onStatus(Status status)
            {
                if (tweetCount < FILE_QUANTITY)
                {
                    PrintWriter out = new PrintWriter(bw);
                    out.println(TwitterObjectFactory.getRawJSON(status));
                    tweetCount++;
                    if (tweetCount % 100 == 0)
                        System.out.println(tweetCount);
                }
                else
                {
                    try {
                        bw.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(tweetCount + " tweets saved. ");
                    TweetSampleCollector.stop();
                }
            }

            @Override
            public void onException(Exception ex)
            {
                ex.printStackTrace();
            }

            /* Unimplemented Methods */
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}
            @Override
            public void onStallWarning(StallWarning warning) {}
        });
        twitterStream.sample("en");
    }
}
