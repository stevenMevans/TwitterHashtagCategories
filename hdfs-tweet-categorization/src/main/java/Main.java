import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.hadoop.conf.Configuration;
import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URI;

public class Main
{
    public static final URI HDFS = URI.create("hdfs://localhost:8020/popular-hashtags/");
    public static final URI LOCAL_INPUT = URI.create("src/data/tweets.json");
    public static final URI LOCAL_OUTPUT = URI.create("src/data/output");

    public static Configuration globalConf = new Configuration();

    public static void main(String[] args)
    {
//        try
//        {
//            HashtagCounter.count();
//            Categorizer.categorize();
//        }
//        catch (Exception ex)
//        {
//            ex.printStackTrace();
//        }

        String keyword = args[0];
        String attribute = args[1];

        int wordCount = 0;
        try
        {
            wordCount = KeywordCounter.count(keyword, attribute);
        }
        catch (InvalidArgumentException | IOException | TwitterException e)
        {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        System.out.println("\n Keyword \"" + keyword + "\" appears " + wordCount + " times within the " + attribute + " attribute across all 12 directories. \n");
    }
}
