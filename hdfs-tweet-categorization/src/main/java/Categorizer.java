import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class Categorizer
{
    private static final String JOB_NAME = "CategorizeTweets";

    /*
     * Processes a local JSON file containing Twitter object information and creates
     * directories and writes files containing JSON data in an HDFS directory based on
     * the hashtags contained in the Twitter JSON object.
     */
    public static void categorize() throws Exception {
        FileSystem hdfs = FileSystem.get(Main.HDFS, Main.globalConf);

        Job job = Job.getInstance(Main.globalConf, JOB_NAME);
        job.setJarByClass(Categorizer.class);

        job.setMapperClass(Categorizer.Map.class);
        job.setReducerClass(Categorizer.Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Main.LOCAL_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Main.HDFS));

        if (hdfs.exists(new Path(Main.HDFS)))
            hdfs.delete(new Path(Main.HDFS), true);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text content = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            try {
                content.set(line);
                Status status = TwitterObjectFactory.createStatus(line);
                HashtagEntity[] hashtags = status.getHashtagEntities();
                if (hashtags == null || hashtags.length == 0) {
                    word.set("None");
                    context.write(word, content);
                }
                else {
                    for (HashtagEntity hashtag : hashtags) {
                        if (HashtagCounter.isTopHashTag(hashtag.getText()))
                            word.set(hashtag.getText());
                        else
                             word.set("Others");
                        context.write(word, content);
                    }
                }
            } catch (TwitterException e) { e.printStackTrace(); }
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> out;

        public void setup(Context context) {
            out = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values)
                out.write(NullWritable.get(), value, key.toString() + "/tweets");
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }
}
