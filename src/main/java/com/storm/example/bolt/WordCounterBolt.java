package com.storm.example.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt implements IRichBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        if(!counters.containsKey(str)){
            counters.put(str,1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "]");
        for(Map.Entry<String, Integer> entry: counters.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
