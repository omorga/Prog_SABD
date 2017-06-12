package query;

import model.GenStats;
import model.Genres;
import model.SingleStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


public class Query2 {
	public static class RatingsMapper extends Mapper<Object, Text, Text, Text> {
 
        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
        	//Il file viene letto per righe e splittato a ogni virgola.
            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");

            Genres genre = new Genres();
            
            if (!parts[3].equals("timestamp")){
                genre.setRating(Float.parseFloat(parts[2]));
                if (genre.getRating() != null){
	            	//la chiave sar� l'id del film e il valore la valutazione
                	context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(genre)) );
                }
            }
        }
    }

    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//Il file viene letto per righe e splittato a ogni virgola.Inoltre viene effettuato un ulteriore controllo per
        	//evitare errore di parsing, qualora il titolo di un film presenti delle virgole.
        	
            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");    	
            String[] generi = parts[parts.length - 1].split("\\|");
            Genres genre = new Genres();
            genre.setName(generi);
           
            if(parts[1]!="title"){
            	//la chiave sar� l'id del film e il valore il nome del film
            	context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(genre)) );
            }
  
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();
        

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	//il contex viene raggruppato per chiave e ogni valore sar� una classe Genres
            // ciascuna classe presenter� la lista dei generi di un film o il rating del film, a seconda da quale file � stata mappata la classe
        	
        	int i;
        	float sum = 0;
        	float sumsq = 0;
            int count = 0;
            String[] differentGenres = null;
            for (Text text : values) {
                Genres genre = mapper.readValue(text.toString(), Genres.class);
                if (genre.getName() != null){
                	differentGenres=genre.getName(); //se la lista di generi � diversa da null la setto
                	     
                }else if (genre.getRating()!=null){  //altrimenti calcolo la somma dei quadrati , la somma dei rating e calcolo il numero di voti
                    sumsq= sumsq + genre.getRating() * genre.getRating();
                    sum = sum + genre.getRating();
                    count++;
                }
                
            }if(count!=0){
	            for(i=0;i<differentGenres.length;i++){
	            	// nel contex venfono scritti i diversi generi con la rispettiva somma, somma dei quadrati e il contatore
	            	context.write(new Text(differentGenres[i]+","), new Text(sum + "," + sumsq + "," + count));
	
	            }
            }
        }
    }
    
    public static class SplitMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//Il file viene letto per righe e splittato a ogni virgola.
            String line = value.toString().toLowerCase();
            String[] parts = line.split(",");
            
            SingleStats st = new SingleStats();
            
            st.setCount(Integer.parseInt(parts[3]));
            st.setSum(Float.parseFloat(parts[1]));
            st.setSumsq(Float.parseFloat(parts[2]));
        	//la chiave sar� il genere e il valore la somma di voti per il genere, la somma dei rating e la somma dei qudrati dei rating
            context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(st)));
  
        }
    }
    
    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	GenStats returnGt = new GenStats();
        	int count = 0;
        	float sum = 0;
        	float sumsq = 0;
        	
        	//il contex � raggruppato per generi, per ogni genere calcoliamo la somma totale dei rating, la somma totale dei voti
        	// e la somma totale dei quadrati
        	for (Text text : values) {
        		SingleStats st = mapper.readValue(text.toString(), SingleStats.class);	
        		count = count + st.getCount();
        		sum = sum + st.getSum();
        		sumsq = sumsq + st.getSumsq();
        		
        		
        	}
        	//per calcolare la media facciamo la somma totale dei ranting fratto il numero di voti
        	returnGt.setAvg(sum/count);
        	
        	//per cacolare la deviazione standard utilizziamo la seguente formula sqrt((A-B^2/C)/C-1)
        	//A= somma dei quadrati dei rating
        	//B= somma dei rating
        	//C= somma dei voti
        	float dv =  (float) Math.sqrt((sumsq-sum*sum/count)/(count-1));
        	returnGt.setDev(dv);
        	//nel contex verr� scritto il il genere con la sua media e la sua deviazione standard
        	context.write(key, new Text(mapper.writeValueAsString(returnGt)));
            
            
        }
    }
   
    public static void main(String[] args) throws Exception {

    	//creazione e configurazione del primo job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "split");
        job1.setJarByClass(Query2.class);
        
        //Creazione di due classe Mapper in quanto in input abbiamo due file.
        MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, RatingsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, GenreMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        //Creazione classe Reducer
        job1.setReducerClass(GenreReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.setOutputFormatClass(TextOutputFormat.class);

        //attendiamo il completameto del primo job
        int code = job1.waitForCompletion(true) ? 0 : 1;

        
        if (code == 0) {
        	//creazione e configurazione del secondo job
        	
            Job job2 = Job.getInstance(conf, "compute");
            job2.setJarByClass(Query2.class);
            job2.setMapperClass(SplitMapper.class);
            
            //Creazione classe Reducer
            job2.setReducerClass(StatsReducer.class);
            job2.setNumReduceTasks(2);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[2] + "/part-r-00000"));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            
        	//attendiamo la terminazione del secondo job
            code = job2.waitForCompletion(true) ? 0 : 2;
        }

        System.exit(code);

    }
}
