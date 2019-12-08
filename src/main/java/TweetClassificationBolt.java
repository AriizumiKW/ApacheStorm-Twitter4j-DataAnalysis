import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.string__init;
import clojure.asm.Label;
import twitter4j.Status;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;

import javax.swing.text.html.HTML.Tag;

import org.apache.log4j.chainsaw.Main;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class TweetClassificationBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String keywordsCON[] = 
    	{"conservative","con","boris","johnson"}; // Conservative Party and leader name
    private String keywordsLAB[] = 
    	{"labour","jeremy","corbyn"}; // Labour Party & Co-operative Party and leader name
    private String keywordsLDM[] = 
    	{"liberal","libdem","ldm","joanne","swinson"}; // Liberal Democrats and leader name
    private String keywordsGRNandBRX[] = 
    	{"green","grn","brx","nigel","farage"}; // Green Party and Brexit Party and leader name
    
    public static final int CONSER = 1;
    public static final int LABOUR = 2;
    public static final int LIBDEM = 3;
    public static final int GRNBRX = 4;
    public static final int NOT_FOUND = 5;
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	Status status = (Status) input.getValueByField("tweet");
    	String text = status.getText();
    	String[] wordsList = wordFilter(text);
    	
    	int category = 0;
    	outterloop: for(String word: wordsList) {
    		for(String keyword: keywordsCON) {
				if(word.contains(keyword)) {
    				category = CONSER;
    				break outterloop;
    			}
    		}
    		for(String keyword: keywordsLAB) {
    			if(word.contains(keyword)) {
    				category = LABOUR;
    				break outterloop;
    			}
    		}
    		for(String keyword: keywordsLDM) {
    			if(word.contains(keyword)) {
    				category = LIBDEM;
    				break outterloop;
    			}
    		}
    		for(String keyword: keywordsGRNandBRX) {
    			if(word.contains(keyword)) {
    				category = GRNBRX;
    				break outterloop;
    			}
    		}
    		category = NOT_FOUND;
    	}
    	
    	if(category != NOT_FOUND) {
    		collector.emit(new Values(category,wordsList));
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("category","wordslist"));
    }
    
    private String[] wordFilter(String tweet) {
    	// filter word. Get word's list, which we care about
    	String step1 = tweet.split(",")[3]; // split by ','
    	String step2 = step1.toLowerCase(); // to lower case
    	String step3 = step2.replaceAll("[^a-zA-Z\\s]", " "); 
    	// replace all non-alphabet character by one spacing ' '
    	String step4 = step3.replaceAll("\\s{2,}", " ");
    	// if there are more than 2 spacing, replace it by one spacing ' '
    	if(step4.startsWith(" ")) {
    		step4 = step4.substring(1); // if it starts with spacing, remove
    	}
    	String[] step5 = step4.split("\\s"); // split by spacing ' '
    	return step5; // the tweet's words list
    }
}
