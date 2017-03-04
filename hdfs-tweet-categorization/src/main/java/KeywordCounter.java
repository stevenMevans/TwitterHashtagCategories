import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.*;

public class KeywordCounter
{
    /*
     * Searches several HDFS directories for a given keyword that
     * may be stored in one of two attribute fields of a Twitter
     * JSON object and counts the number of occurrences for that
     * keyword. The results are stored in a local output file.
     */
    public static int count(String keyword, String attribute) throws InvalidArgumentException, IOException, TwitterException
    {
        int count = 0;
        FileSystem hdfs = FileSystem.get(Main.HDFS, Main.globalConf);
        FileStatus[] statuses = hdfs.listStatus(new Path(Main.HDFS.getPath()));
        for (FileStatus status : statuses)
        {
            if (status.isDirectory())
            {
                FileStatus[] fileStatuses = hdfs.listStatus(new Path(status.getPath().toUri()));
                for (FileStatus fileStatus : fileStatuses)
                {
                    BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(fileStatus.getPath().toUri()))));
                    String line;
                    line = br.readLine();
                    while (line != null)
                    {
                        Status tweet = TwitterObjectFactory.createStatus(line);
                        if ("hashtags".equals(attribute.toLowerCase()))
                        {
                            for (HashtagEntity hashtag : tweet.getHashtagEntities())
                                if (hashtag.getText().toLowerCase().contains(keyword.toLowerCase()))
                                    count++;
                        }
                        else if ("text".equals(attribute.toLowerCase()))
                        {
                            if (tweet.getText().toLowerCase().contains(keyword.toLowerCase()))
                                count++;
                        }
                        else
                        {
                            throw new InvalidArgumentException(new String[]{"The attribute argument given is invalid."});
                        }
                        line = br.readLine();
                    }
                }
            }
        }
        return count;
    }
}
