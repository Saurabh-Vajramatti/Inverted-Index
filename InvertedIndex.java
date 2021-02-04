import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class InvertedIndex{
        public static class Map extends Mapper<LongWritable, Text, Text, Text>
        {
                private Text word = new Text();
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        // Bringing all the characters to lowercase
                        String line = value.toString().toLowerCase();

                        // Splitting into doc number and its text
                        String document_split[] = line.split("\t",2);
                        Text document_number = new Text(document_split[0]);

                        // Replace all non alphabetic characters and non spaces with a space
                        String document_content=document_split[1].replaceAll("[^ a-z]+", " ");

                        // Tokenize into words
                        StringTokenizer tokenizer = new StringTokenizer(document_content);
                        while(tokenizer.hasMoreTokens())
                        {
                                word.set(tokenizer.nextToken());
                                context.write(word, document_number);
                        }
                }
        }
        public static class Reduce extends Reducer<Text, Text, Text, Text>
        {
                @Override
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                         
                         HashMap<String, Integer> documentID_to_count_dictionary = new HashMap();
                         for(Text document_number: values)
                         {
                                String temp_doc_number = document_number.toString();
                                if(documentID_to_count_dictionary.containsKey(temp_doc_number)==false)
                                 {
                                        //  Making a new entry in the dictionary
                                        Integer initial_count_integer_object=new Integer(1);
                                       documentID_to_count_dictionary.put(temp_doc_number,  initial_count_integer_object);
                                 }
                                 else
                                 {
                                        //  Get the corresponding count
                                         int count = documentID_to_count_dictionary.get(temp_doc_number);
                                        //  Increment by 1
                                        count+=1;
                                        // Creating the corresponding integer object
                                         Integer count_integer_object = new Integer(count);
                                        //  Assigning new value
                                        documentID_to_count_dictionary.put(temp_doc_number, count_integer_object);			 
                                }
                         }
                         StringBuilder postings_list_stringbuilder = new StringBuilder();
                        postings_list_stringbuilder.append("");
                        for(HashMap.Entry<String, Integer> dictionary_record : documentID_to_count_dictionary.entrySet())
                        {
                                String document_id = dictionary_record.getKey();
                                Integer count_object = dictionary_record.getValue();
                                postings_list_stringbuilder.append(document_id);
                                postings_list_stringbuilder.append(":");
                                String count_string = count_object.toString();
                                postings_list_stringbuilder.append(count_string);
                                postings_list_stringbuilder.append(" ");
                        }
                        String postings_list_string=postings_list_stringbuilder.toString();
                        context.write(key,new Text(postings_list_string));
                }
        }
        public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
        {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "invertedindex");
                job.setJarByClass(InvertedIndex.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setMapperClass(Map.class);
                job.setReducerClass(Reduce.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.waitForCompletion(true);
        }
}