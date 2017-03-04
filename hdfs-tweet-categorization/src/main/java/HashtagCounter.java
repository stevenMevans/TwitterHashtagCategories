import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jetbrains.annotations.Nullable;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class HashtagCounter
{
    private static final int MAX_SIZE = 10;
    private static final String JOB_NAME = "CountHashtags";

    public static HashMap<String, IntWritable> mostUsed = new HashMap<>(MAX_SIZE);

    /*
     * Processes a JSON file with Twitter object data and uses MapReduce to count the
     * number of occurrences of each hashtag and prints the results to an output directory.
     * The most frequently occurring hashtags are stored in a map along with their counts.
     */
    public static void count() throws Exception
    {
        FileSystem hdfs = FileSystem.get(Main.globalConf);

        Job job = Job.getInstance(Main.globalConf, JOB_NAME);
        job.setJarByClass(HashtagCounter.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Main.LOCAL_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Main.LOCAL_OUTPUT + "/hashtag-frequencies"));

        if (hdfs.exists(new Path(Main.LOCAL_OUTPUT + "/hashtag-frequencies")))
            hdfs.delete(new Path(Main.LOCAL_OUTPUT + "/hashtag-frequencies"), true);

        job.waitForCompletion(true);
        System.out.println(mostUsed.toString());
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            word.clear();
            String line = value.toString().trim();

            try {
                Status status = TwitterObjectFactory.createStatus(line);
                HashtagEntity[] hashtags = status.getHashtagEntities();
                for (HashtagEntity hashtag : hashtags) {
                    word.set(hashtag.getText());
                    context.write(word, one);
                }
            } catch (TwitterException tE) { tE.printStackTrace(); }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();

            addToMostUsed(key.toString(), sum);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void addToMostUsed(String key, int count) {
        IntWritable result = new IntWritable(count);

        if (mostUsed.size() < MAX_SIZE)
            mostUsed.put(key, result);
        else {
            java.util.Map.Entry smallestEntry = getSmallestEntry();
            IntWritable smallestValue = (IntWritable) smallestEntry.getValue();
            if (smallestValue.compareTo(result) < 0) {
                mostUsed.remove(smallestEntry.getKey());
                mostUsed.put(key, result);
            }
        }
    }

    @Nullable
    public static java.util.Map.Entry getSmallestEntry() {
        if (mostUsed.isEmpty())
            return null;

        Iterator iterator = mostUsed.entrySet().iterator();
        if (iterator == null)
            return null;

        java.util.Map.Entry smallestEntry = (java.util.Map.Entry) iterator.next();
        while (iterator.hasNext()) {
            IntWritable smallestValue = (IntWritable) smallestEntry.getValue();
            java.util.Map.Entry entryToCheck = (java.util.Map.Entry) iterator.next();
            IntWritable valueToCheck = (IntWritable) entryToCheck.getValue();
            if (valueToCheck.compareTo(smallestValue) < 0)
                smallestEntry = entryToCheck;
        }

        return smallestEntry;
    }

    public static boolean isTopHashTag(String hashtag)
    {
        return mostUsed.containsKey(hashtag);
    }
}
