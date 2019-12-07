import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterStreamSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;
		
		StatusListener listener = new StatusListener() {
			 
	        @Override
	        public void onException(Exception e) {
	            e.printStackTrace();
	        }
	        @Override
	        public void onDeletionNotice(StatusDeletionNotice arg) {
	        }
	        @Override
	        public void onScrubGeo(long userId, long upToStatusId) {
	        }
	        @Override
	        public void onStallWarning(StallWarning warning) {
	        }
	        @Override
	        public void onStatus(Status status) {
	        	queue.offer(status);
	        }
	        @Override
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	        }
	    };
	    ConfigurationBuilder configBuilder = new ConfigurationBuilder();
	    configBuilder.setDebugEnabled(true);
	    configBuilder.setOAuthConsumerKey("vxsU6r1TsjGEOMCDVSxKJUeow");
	    configBuilder.setOAuthConsumerSecret("AHpg6VkWGC3zxgtkSOHMNmyQ8tq5VdXcGNPNTS0NbudxdXFnVN");
	    configBuilder.setOAuthAccessToken("1198943607690080256-RhaS83OxaYLuh3O2Qed8u9KPn0b16n");
	    configBuilder.setOAuthAccessTokenSecret("ZHSp7P3fSPaOyZishLrszkvvvHdFYlbNH1kGSrhVcxBIP");
	 
	    TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
	    twitterStream.addListener(listener);
	    
	    String keywords[] = {"#GE2019","#GE19","#generalelection","#generalelection2019",
	    		"#generalelection19","#GeneralElection19","#GeneralElection2019",
	    		"#VoteTactical","#VoteTactically"};
	    
	    FilterQuery fq = new FilterQuery();
	    fq.track(keywords);
	    twitterStream.filter(fq);
	    //twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		
		Status oneText = queue.poll();
		
		if(oneText == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(oneText));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
