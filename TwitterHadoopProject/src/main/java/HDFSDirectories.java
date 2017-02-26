import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

public class HDFSDirectories
{
    public static void categorize(Map<String, IntWritable> topHashtags) throws IOException, URISyntaxException
    {
        URI uri = URI.create("hdfs://localhost:8020/");
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(uri, conf);

        for (Map.Entry entry : topHashtags.entrySet())
            hdfs.mkdirs(new Path(URI.create("/" + entry.getKey())));

        URI othersDir = URI.create("/Others");
        hdfs.mkdirs(new Path(othersDir));

        URI noneDir = URI.create("/None");
        hdfs.mkdirs(new Path(noneDir));
    }
}
