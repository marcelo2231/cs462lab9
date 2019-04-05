ruleset io.picolabs.gossip {
  meta {
    shares __testing, state, temp_logs, getPeer, getMessage, send, update, smart_tracker, sequence_id
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias subscriptions
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
      { "name": "state" },
      { "name": "temp_logs" },
      { "name": "getPeer" },
      { "name": "getMessage" },
      { "name": "send" },
      { "name": "update" },
      { "name": "sequence_id" },
      { "name": "smart_tracker" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ { "domain": "gossip", "type": "clear" },
        { "domain": "gossip", "type": "fake_data" },
        { "domain": "gossip", "type": "fake_data_empty" },
        { "domain": "gossip", "type": "run" },
        { "domain": "gossip", "type": "heartbeat" },
        { "domain": "gossip", "type": "new_temperature", "attrs": [ "temperature", "timestamp" ] }
      ]
    }
     getPeer = function(){
       mysubscriptions = subscriptions:established();
       allThatNeedUpdate = mysubscriptions.map(function(x){
          eci = x["Tx"].klog("Tx");
          url = "http://localhost:8080/sky/cloud/" + eci + "/io.picolabs.gossip/state";
          
          content = http:get(url){"content"}.decode();
          ent:state.map(function(v, k){
            value = content{k}.klog("value");
            valueNotFound = (value.isnull()).klog("valueNotFound");
            valueHigher = (value < v).klog("valueHigher");
            needUpdate = (valueHigher || valueNotFound).klog("needUpdate");
            value = (valueNotFound) => 0 | value;
            ((needUpdate) => value | null).klog("updatingResult");
          }).append(x{"Tx"});
       });
      allThatNeedUpdate.klog("allThatNeedUpdate");
      finalResult = allThatNeedUpdate.filter(function(v, k){
        filterTwo = v.filter(function(v1, k1){
          (not v1.values()[0].isnull())
        }).klog("vfilter");
        ((filterTwo != {}) && filterTwo != []);
      }).klog("finalResult");
      // Get first one
       subscriber = finalResult.reduce(function(a,b){
         a
       });
       ((subscriber.values()[0] == {}) => null | subscriber);
       
     };
     
     getMessage = function(subscriber){
       messagesFromThatECI = ent:smart_tracker.filter(function(v, k){
        filterResult = v.filter(function(v, k){
          k11 = subscriber.filter(function(v1, k1){ k1 == v.klog("v") }).klog("k1");
          k11 != {}
        }).klog("resullltt");
        filterResult != {}
      });
      
      amount = subscriber.values()[0] + 1;
      message__ID = subscriber.keys()[0]+":"+amount;
      message__ID.klog("message__ID");
      
      messagesFromThatECI.filter(function(v, k){
        filterResult = v.filter(function(v, k){
          k11 = subscriber.filter(function(v1, k1){ message__ID == v.klog("v") }).klog("k1");
          k11 != {}
        }).klog("resullltt");
        filterResult != {}
      });
      
     };
    

    state = function(){
      ent:state.defaultsTo({})
    } 
    temp_logs = function(){
      ent:temp_logs.defaultsTo({})
    } 
    smart_tracker = function(){
      ent:smart_tracker.defaultsTo({})
    }
    sequence_id = function(){
      ent:sequecence_id.defaultsTo(0)
    }
  }
  
  
  rule run{
    select when gossip run
    fired{
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": 15})
    }
  }
  
  rule gossip_heartbeat{
    select when gossip heartbeat
    pre{
      subscriber = getPeer().klog("SUBSCRIBERRR")
      m = getMessage(subscriber).klog("MESSAGE")
      eci = subscriber[1].klog("ECI")
      messageFrom = m[0]{"SensorID"}.klog("MESSAGEFROM")
      valid = ((not subscriber.isnull()) && (not eci.isnull())).klog("VALID")
    }
    if valid then
      event:send(
        { "eci": eci, "eid": "send-message",
          "domain": "gossip", "type": "rumor",
          "attrs": { 
                      "message" : m[0],
                      "messageFrom": messageFrom
                   }
        });
    always{
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": 15})
    }
  }
  
  
  rule gossip_rumor{
    select when gossip rumor
    pre{
      message = event:attr("message").klog("Message");
      messageFrom = event:attr("messageFrom").klog("message")
    }
    
    fired{
      ent:state{messageFrom} := ent:state{messageFrom}.defaultsTo(0) + 1;
      ent:smart_tracker := ent:smart_tracker.append(message);
    }
  }
  
    rule gossip_seen{
    select when gossip seen
    pre{
      
    }
    
    fired{
      
    }
  }
  
  
  rule new_temperature{
    select when gossip new_temperature
    pre{
      temperature = event:attrs{"temperature"}
      timestamp = event:attrs{"timestamp"}
      sensor_id = meta:picoId
      sequence_id = ent:sequence_id.defaultsTo(0) + 1
      message_id = sensor_id + ":" + sequence_id
      
    }
    always{
      ent:smart_tracker := ent:smart_tracker.append({"MessageID": message_id,
     "SensorID": sensor_id,
     "Temperature": temperature,
     "Timestamp": timestamp});
      
      ent:state := ent:state.put(sensor_id,sequence_id);
      ent:sequence_id := sequence_id
    }
  }
  
  rule clear_all{
    select when gossip clear
    always{
      ent:sequence_id := 0;
      ent:state := {};
      ent:smart_tracker := {};
      ent:temp_logs := {};
    }
  }
  
  rule fake_data{
    select when gossip fake_data
    always{
      //fake = (ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"} == null) => {} | ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"};
      //ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"} := fake.put("cju0btcwi001ic4vvy73ib4j9", 0);
      ent:state{"cju0btcwi001ic4vvy73ib4j9"} := -1;
    }
  }
  
    rule fake_data_empty{
    select when gossip fake_data_empty
    always{
      //fake = (ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"} == null) => {} | ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"};
      //ent:temp_logs{"cju0btcwi001ic4vvy73ib4j9"} := "";
      ent:state{"cju0btcwi001ic4vvy73ib4j9"} := "";
    }
  }
}
