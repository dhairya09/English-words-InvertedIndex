import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InvertedIndex {
        public static class TMapper
        extends Mapper<Object, Text, Text, Text>{
                //private final static IntWritable one = new IntWritable(1);
                private Text wrd = new Text();
                private Text dId = new Text();
                public void map(Object key, Text value, Context context
                                ) throws IOException, InterruptedException {
                        StringTokenizer iterator = new StringTokenizer(value.toString());
                        dId.set(iterator.nextToken());
                        while (iterator.hasMoreTokens()) {
                                wrd.set(iterator.nextToken());
                                context.write(wrd, dId);
                        }
                }
        }
        public static class TReducer
        extends Reducer<Text,Text,Text,Text> {
                private Text fmap = new Text();
                public void reduce(Text key, Iterable<Text> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        HashMap<String, Integer> hmap = new HashMap<String, Integer>();
                        for (Text val : values) {
                                if(hmap.containsKey(val.toString())){
                                hmap.put(val.toString(),hmap.get(val.toString())+1);
                        }else{
                                hmap.put(val.toString(),1);
                        }
                        }
                        StringBuilder sb = new StringBuilder();
                        for(Map.Entry<String, Integer> pair : hmap.entrySet()){
                                sb.append(pair.getKey());
                                sb.append(":");
                                sb.append(pair.getValue());
                                sb.append("\t");
                        }
                        fmap.set(sb.toString());
                        context.write(key, fmap);
                }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job j = Job.getInstance(conf, "word count");
                j.setJarByClass(InvertedIndex.class);
                j.setMapperClass(TMapper.class);
                j.setReducerClass(TReducer.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(j, new Path(args[0]));
                FileOutputFormat.setOutputPath(j, new Path(args[1]));
                System.exit(j.waitForCompletion(true) ? 0 : 1);
        }
        }
}
