package query;

import model.AvgRating;
import model.Ranking;
import model.Rating;
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


public class Query3 {

    public static class TimestampFilterMapper extends Mapper<Object, Text, Text, Text> {

        private final static long Year1Start = 1364774400;// 1 Aprile 2013
        private final static long Year1End = 1396224000; //31 Marzo 2014
        private final static long Year2Start = 1396310400; //1 Aprile 2014
        private final static long Year2End = 1427760000; //31 Marzo 2015
        
        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	
        	//Il file viene letto per righe e splittato a ogni virgola.
            String line = value.toString().toLowerCase();

            String[] parts = line.split(",");
            Rating rating = new Rating();
            
            //vengono considerati solo i voti assegnati dal 1 Aprile 2013 al 31 Marzo 2014
            if (!parts[3].equals("timestamp") && Long.parseLong(parts[3]) > Year1Start && Long.parseLong(parts[3]) < Year1End)
            {
                rating.setRating(Float.parseFloat(parts[2]));
                if (rating.getRating() != null){
                	rating.setYear(1);
                    context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(rating)) );
                }
            }
            //vengono considerati solo i voti assegnati dal 1 Aprile 2014 al 31 Marzo 2015
            else if (!parts[3].equals("timestamp") && Long.parseLong(parts[3]) > Year2Start && Long.parseLong(parts[3]) < Year2End)
            {
            	rating.setRating(Float.parseFloat(parts[2]));
                if (rating.getRating() != null){
                	rating.setYear(2);
                	// viene scritto l'id del film e il valore la valutazione con il periodo in cui � stata effettuata
	            	//1 per il primo periodo
	            	//2 per il secondo periodo
                    context.write(new Text(parts[1]),new Text(mapper.writeValueAsString(rating)) );
                }
            }
        }
    }

    public static class MovieTitleMapper extends Mapper<Object, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            //Il file viene letto per righe e splittato a ogni virgola.Inoltre viene effettuato un ulteriore controllo per
        	//evitare errore di parsing, qualora il titolo di un film presenti delle virgole.
            
            String[] parts = line.split(",");
            if(parts[1] != "title"){
            	if(parts.length > 3){
            		int i;
            		for(i=2;i<parts.length-1;i++)
            			parts[1] = parts[1].concat(parts[i]);
            	}
            	
	            Rating rating = new Rating();
	            rating.setTitle(parts[1]);
	        	//la chiave sar� l'id del film e il valore il nome del film
	            context.write(new Text(parts[0]), new Text(mapper.writeValueAsString(rating)) );
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        private final static ObjectMapper mapper = new ObjectMapper();
        private final static Ranking Chart1 =new Ranking(10); //classifica primo periodo
        private final static Ranking Chart2 = new Ranking(10); //classifica secondo periodo
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum1 = 0;
            int count1 = 0;
            int sum2 = 0;
            int count2 = 0;
            String title = null;
            
            //il contex viene raggruppato per chiave e ogni valore sar� una classe Rating
            // ciascuna classe presenter� il titolo del film o il rating e il periodo, a seconda da quale file � stata mappata la classe
            for (Text text : values) {
                Rating rating = mapper.readValue(text.toString(), Rating.class);
                if (rating.getTitle() != null){  //se il titolo � diverso da null setto il titolo
                    title = rating.getTitle();
                }
                else if (rating.getRating()!=null && rating.getYear() ==1){  //se il periodo � uguale a uno calcolo la somma dei rating e conto i voti
                    sum1 += rating.getRating();
                    count1++;
                }
                else if (rating.getRating()!=null && rating.getYear() ==2){ //se il periodo � uguale a due calcolo la somma dei rating e conto i voti
                    sum2 += rating.getRating();
                    count2++;
                }
            }
            float avg1 = ((float) sum1 / (float) count1); //calcolo la media del film per il primo periodo
            float avg2 = ((float) sum2 / (float) count2); //calcolo la media del film per il secondo periodo
            if (count1 >=50){		//considero solo se i voti sono maggiori di 50
            	AvgRating returnAvg1 = new AvgRating();
            	returnAvg1.setAvg(avg1);
            	returnAvg1.setTitle(title);
                Chart1.getAvgRating().add(returnAvg1); //Aggiorno la classifica globale del primo periodo
                if(Chart1.overMaxSize()){			//se ho superato i 10 film elimino l'elemento ultimo della lista
                	Chart1.getAvgRating().pollLast();
                }
            }
            if (count2 >=50){ //considero solo se i voti sono maggiori di 50
                AvgRating returnAvg2 = new AvgRating();
                returnAvg2.setAvg(avg2);
                returnAvg2.setTitle(title);
                Chart2.getAvgRating().add(returnAvg2); //Aggiorno la classifica globale del secondo periodo
                if(Chart2.overMaxSize()){			//se ho superato i 10 film elimino l'elemento ultimo della lista
                	Chart2.getAvgRating().pollLast();
                }
            }
            
           
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	//Questa funzione viene eseguita dopo il reduce e ha il compito di confrontare le due classifiche
            int i = 0;
            for ( AvgRating k : Chart2.getAvgRating()) {
                int j = 0;
                	Integer app = null;
            		for(AvgRating h : Chart1.getAvgRating()){
            			Chart1.getAvgRating().iterator();
            			
            			if(h.getTitle() == k.getTitle()){
            				app = (-1)*(i-j);
            				break;
            			}
            			j++;
            		}
            		if(app !=null){
                    	//Se posso confrontare calcolo la differenza delle posizioni
            			context.write(new Text(k.getTitle()), new Text("Ultimo anno: " + k.getAvg() +  " diff pos: " + app.toString()));
            		}else{
                    	//Se non posso confrontare non dichiaro.
            			context.write(new Text(k.getTitle()), new Text("Ultimo anno: " + k.getAvg() +  " diff pos: " + " ND"));
            		}
            		i++;
            }
        }
    }
 
    public static void main(String[] args) throws Exception {

        //Creazione nuova configurazione job MapReduce
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "compute ranking");
        job.setJarByClass(Query1.class);

        //Creazione di due classe Mapper in quanto in input abbiamo due file.
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, TimestampFilterMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MovieTitleMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //Creazione classe Reducer
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        //Attendiamo la terminazione del job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}