package com.storm.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.storm.example.bolt.WordCounterBolt;
import com.storm.example.bolt.WordNormalizerBolt;
import com.storm.example.spout.WordReaderSpout;

public class MainTopology {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-reader-spout", new WordReaderSpout());
        builder.setBolt("word-normalizer-bolt", new WordNormalizerBolt()).shuffleGrouping("word-reader-spout");
        builder.setBolt("word-counter-bolt", new WordCounterBolt(), 2).fieldsGrouping("word-normalizer-bolt", new Fields("word"));

        Config conf = new Config();

        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Getting-started-topology", conf, builder.createTopology());
        Thread.sleep(1000l);
        cluster.shutdown();

    }
}
