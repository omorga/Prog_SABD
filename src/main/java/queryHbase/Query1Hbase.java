package queryHbase;

import hbaseClient.HBaseClient;
import model.Movie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Query1Hbase {
	
	static HBaseClient hbc;

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

		//Timestamp corrispondente al 1 gennaio 2010
	    private final static long thresholdTimestamp = 946684800;
	    private final static ObjectMapper mapper = new ObjectMapper();
	
	    @Override
	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	//Il file viene letto per righe e splittato a ogni virgola.
	        String line = value.toString().toLowerCase();
	        String[] parts = line.split(",");
	     
	        Movie movie = new Movie();
	        
	        // vengono considerate solo le valutazioni effettuale dal 1 gennaio 2010
	        if (!parts[3].equals("timestamp") && Long.parseLong(parts[3]) > thresholdTimestamp)
	            movie.setRating(Float.parseFloat(parts[2]));
	            if (movie.getRating() != null){
	            	//la chiave sar� l'id del film e il valore la valutazione
	                context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(movie)) );
	            }
	
	    }
    }

    public static class MovieTitleMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

        	//Il file viene letto per righe e splittato a ogni virgola.Inoltre viene effettuato un ulteriore controllo per
        	//evitare errore di parsing, qualora il titolo di un film presenti delle virgole.
        	
            String line = value.toString().toLowerCase(); 
            String[] parts = line.split(",");
            if(parts[1] != "title"){
            	if(parts.length > 3){
            		int i;
            		for(i=2;i<parts.length-1;i++)
            			parts[1] = parts[1].concat(parts[i]);
            	}	
            }
            Movie movie = new Movie();
            movie.setTitle(parts[1]);
            
        	//la chiave sar� l'id del film e il valore il nome del film
            context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(movie)) );
        }
    }

    public static class FilterAverageReducer extends Reducer<Text, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Movie returnMovie = new Movie();
            
            //il contex viene raggruppato per chiave e ogni valore sar� una classe Movie
            // ciascuna classe presenter� il titolo del film o il rating, a seconda da quale file � stata mappata la classe
            for (Text text : values) {
                Movie movie = mapper.readValue(text.toString(), Movie.class);
                if (movie.getTitle() != null){ //se il titlo � diverso da null,setto il titolo
                    returnMovie.setTitle(movie.getTitle());
                }
                else if (movie.getRating()!=null){ //altrimenti mi calcolo la somma dei rating e incremento il contatore delle valutazioni
                    sum += movie.getRating();
                    count++;
                }
            }
            
            //Infine calcoliamo la media e scriviamo solo quei film che hanno un rating maggiore di 4.
            // Inoltre consideriamo solo quei film che hanno ricevuto pi� di 10 voti
            float threshold = 4;
            float avg = ((float) sum / (float) count);
            if (avg >= threshold  && count >= 10){
                returnMovie.setRating(avg);
                
            	//salvo in Hbase il risultato
				hbc.put("tableQuery1", returnMovie.getTitle(), "fi","id", key.toString(), "fi", "rating",returnMovie.getRating().toString() );

            }

        }
    }

    public static void main(String[] args) throws Exception {

        //Creazione nuova configurazione e un nuovo job MapReduce
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average rating");
        job.setJarByClass(Query1Hbase.class);
       
        hbc = new HBaseClient();
        //creazione tabella HBase
        if (!hbc.exists("tableQuery1")){
            System.out.println("Creating table...");
            hbc.createTable("tableQuery1", "fi");
        }
        //Creazione di due classe Mapper in quanto in input abbiamo due file.
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MovieTitleMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //Creazione classe Reducer
        job.setReducerClass(FilterAverageReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println(hbc.describeTable("tableQuery1"));
        
        //Attendiamo la terminazione del job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}