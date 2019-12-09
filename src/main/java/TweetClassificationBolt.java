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
    private final String keywordsCON[] = 
    	{"conservative","boris","johnson"}; // Conservative Party and leader name
    private final String keywordsLAB[] = 
    	{"labour","jeremy","corbyn"}; // Labour Party & Co-operative Party and leader name
    private final String keywordsLDM[] = 
    	{"liberal","libdem","ldm","joanne","swinson"}; // Liberal Democrats and leader name
    private final String keywordsBRX[] = 
    	{"brexit","brx","nigel","farage"}; // Brexit Party and leader name
    
    public static final int NOT_DEFINE = 0;
    public static final int CONSER = 1;
    public static final int LABOUR = 2;
    public static final int LIBDEM = 3;
    public static final int BREXIT = 4;
    public static final int NOT_FOUND = 5;
    public static final int MORETHAN_TWO_KEYWORD = 6;
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    	Status status = (Status) input.getValueByField("tweet");
    	String text = status.getText();
    	String[] wordsList = wordFilter(text);
    	
    	int category = classify(wordsList.clone());
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
    	String step1 = tweet.toLowerCase(); // to lower case
    	String step2 = step1.replaceAll("[^a-zA-Z\\s]", " "); 
    	// replace all non-alphabet character by one spacing ' '
    	String step3 = step2.replaceAll("\\s{2,}", " ");
    	// if there are more than 2 spacing, replace it by one spacing ' '
    	if(step3.startsWith(" ")) {
    		step3 = step3.substring(1); // if it starts with spacing, remove
    	}
    	String[] step4 = step3.split("\\s"); // split by spacing ' '
    	return step4; // the tweet's words list
    }
    
    private int classify(String[] words) {
    	// The structure of classifier can be seen as a tree (for each word)
    	int category = NOT_DEFINE;
    	outter: for(String word: words) {
    		for(String keyword: keywordsCON) {
    			if(word.contains(keyword)) {
    				if(category == NOT_DEFINE||category == CONSER) {
    					category = CONSER;
    					break;
    				}else {
    					category = MORETHAN_TWO_KEYWORD;
    					break outter;
    				}
    			}
    		}
    		for(String keyword: keywordsLAB) {
    			if(word.contains(keyword)) {
    				if(category == NOT_DEFINE||category == LABOUR) {
    					category = LABOUR;
    					break;
    				}else {
    					category = MORETHAN_TWO_KEYWORD;
    					break outter;
    				}
    			}
    		}
    		for(String keyword: keywordsLDM) {
    			if(word.contains(keyword)) {
    				if(category == NOT_DEFINE||category == LIBDEM) {
    					category = LIBDEM;
    					break;
    				}else {
    					category = MORETHAN_TWO_KEYWORD;
    					break outter;
    				}
    			}
    		}
    		for(String keyword: keywordsBRX) {
    			if(word.contains(keyword)) {
    				if(category == NOT_DEFINE||category == BREXIT) {
    					category = BREXIT;
    					break;
    				}else {
    					category = MORETHAN_TWO_KEYWORD;
    					break outter;
    				}
    			}
    		}
    	}
    	
    	if(category == NOT_DEFINE) {
    		category = NOT_FOUND;
    	}
    	return category;
    }
}
