import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

public class DirectoryCreator
{
    private static final URI uri = URI.create("hdfs://localhost:8020/");
    private static final Configuration conf = new Configuration();

    public static void create(Map<String, IntWritable> topHashtags) throws IOException
    {
        FileSystem hdfs = FileSystem.get(uri, conf);

        for (Map.Entry entry : topHashtags.entrySet())
            hdfs.mkdirs(new Path(URI.create("/" + entry.getKey())));

        URI othersDir = URI.create("/Others");
        hdfs.mkdirs(new Path(othersDir));

        URI noneDir = URI.create("/None");
        hdfs.mkdirs(new Path(noneDir));
    }

    public static void putTweet(String hashtag, String tweet) throws IOException
    {
        FileSystem hdfs = FileSystem.get(uri, conf);

        Path tweetFile = new Path("/" + hashtag + "/tweets.json");
        if (hdfs.exists(tweetFile))
        {
            FSDataOutputStream out = hdfs.create(tweetFile);
        }
        else
        {
            FSDataOutputStream out = hdfs.append(tweetFile);
        }
    }
}
