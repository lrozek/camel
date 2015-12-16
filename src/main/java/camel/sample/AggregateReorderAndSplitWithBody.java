package camel.sample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.aggregate.AggregationStrategy;

/**
 * IMPORTANT
 * Before you run the example copy content of main/resources/camel to c:/temp/camel or change appropriately path in line 81
 */

public class AggregateReorderAndSplitWithBody {

    //NOTE: I am using a small batch value as I do not want to create too many dummy files and I still want to test behavior properly
    private static final int BATCH_SIZE = 2;
    private static final String DUMMY_AGGREGATE_ID_HEADER_NAME = "DummyAggregateId";
    private static final Long DUMMY_AGGREGATE_ID_HEADER_VALUE = Long.valueOf( 1 );
    private static final int NUM_OF_MESSAGES_AFTER_WHICH_AGGREGATOR_WILL_RUN = 1000;
    private static final int TIMEOUT_AFTER_WHICH_AGGREGATOR_WILL_RUN = 1000;

    //If the example runs successfully, then among other logs from Camel there should be following:
    //for given input files and batch size of 2, it shows that sorting worked
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c1_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c1_02.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_02.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c3_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c4_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c5_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c5_02.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c6_01.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c1_03.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c1_04.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_03.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_04.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c5_03.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c5_04.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c1_05.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_05.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_06.txt]
    //########
    //######## Processing file: GenericFile[c:\temp\camel\camelIn\data_c2_07.txt]
    //########

    public static void main( String[] args ) throws Exception {
        CamelContext camelContext = new DefaultCamelContext();

        camelContext.addRoutes( new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                //@formatter:off
                //NOTE: this is not important - this simulates real fetching files, which should already be implemented for real
                //create such folder and copy content of messages.zip into it
                from( "file:c:/temp/camel/camelIn?noop=true" )
                
                //START transformation
                
                //we want to make messages to be ordered in batches of BATCH_SIZE, so we can first process BATCH_SIZE messages for client1, then client2, ... clientX, then back to client1, client2,...
                //to achieve this we will aggregate X messages, resort them in batches, then split them again into separate messages, so they can be processed as before
                //in this way this approach aims to be transparent to currently existing processing code
                
                //first we need to set some header on each Message that will allow for aggregating them together (all Messages with same header will be aggregated into one)
                //FIXME: this is in general not necessary as we want to always group all Messages, but version of aggregate where we do not pass correlactionExpression did not work for me for whatever reason
                //so we are left with setting some "dummy" header value to all Messages
                .process( new Processor() {
                    @Override
                    public void process( Exchange exchange ) throws Exception {
                        exchange.getIn().setHeader( DUMMY_AGGREGATE_ID_HEADER_NAME, DUMMY_AGGREGATE_ID_HEADER_VALUE );
                    }
                })
                
                //FIXME: as aggregator is stateful we should connect it to some persistence mechanism
                //aggregation itself - we aggregate together Messages that have same DUMMY_AGGREGATE_ID_HEADER_NAME (which in our case are all messages)
                //using aggregation strategy CombineResultsAggregationStrategym which just simply adds up all Messages together in a list
                .aggregate( header(DUMMY_AGGREGATE_ID_HEADER_NAME), new CombineResultsAggregationStrategy() )
                  //this parameter is important - it tells aggregator a number of Messages after which it should run aggregation
                  //if value is too small (e.g. '1') then batching is useless, as each Message will be processed by its own
                  //aggregator will not start processing messages until NUM_OF_MESSAGES_AFTER_WHICH_AGGREGATOR_WILL_RUN is achieved (unless timeout is also set - see below)
                  .completionSize( NUM_OF_MESSAGES_AFTER_WHICH_AGGREGATOR_WILL_RUN )
                  //second parameter that controls aggregation
                  //after this timeout aggregator will run anyway, even if NUM_OF_MESSAGES_AFTER_WHICH_AGGREGATOR_WILL_RUN is not achieved
                  //we need this parameter also to avoid situation when there is not enough message and they wait too long not being processed
                  .completionTimeout( TIMEOUT_AFTER_WHICH_AGGREGATOR_WILL_RUN )
                
                //here we are done with aggregation, we have one Message, where all previous Messages are aggregated in a list
                //now we need to sort this list, so content is ordered in batches of BATCH_SIZE per each client
                .process( new Processor() {
                    @Override
                    public void process( Exchange exchange ) throws Exception {
                        //as an input we have a list of all Messages
                        List<Message> messagesInOrderTheyArrived = exchange.getIn().getBody( List.class );
                        //custom class to do sorting in batches does the sort
                        MessageSorter sorter = new MessageSorter(messagesInOrderTheyArrived, BATCH_SIZE);
                        List<Message> sortedMessages = sorter.sort();
                        //we exchange the content of body for a list where files are sorted in batches
                        exchange.getIn().setBody( sortedMessages );
                    }
                })
                
                //here is the splitting of GenericFiles into separate Messages, so processing can resume as it was before 
                .split(body() )
                
                //END transformation
                
                //now we continue with real processing, which should already be implemented for real
                .process( new Processor() {
                    @Override
                    public void process( Exchange exchange ) throws Exception {
                        Object body = exchange.getIn().getBody(  );
                        System.out.print("\n######## Processing file: " + body + "\n########");
                    }
                } );
            }
        } );

        camelContext.start();
        Thread.sleep( 5000 );
        camelContext.stop();
    }
    
    //class that will be used by by aggregator to combine multiple messages into one
    //important is that we do not reuse first incoming message as an aggregation vessel (as most examples I found show)
    //instead ww create a new element that will be used as aggregation vessel
    protected static class CombineResultsAggregationStrategy implements AggregationStrategy {
        @Override
        public Exchange aggregate( Exchange oldExchange, Exchange newExchange ) {
            //this piece of code is executed only for the first element in aggregation
            //IMPORTANT: we do a copy of the first message and use it as an aggregation start
            //if we just reuse newExchange as-is then it will be messed up
            if (oldExchange == null ) {
                oldExchange = newExchange.copy();
                List<Message> newList = new ArrayList<>();
                oldExchange.getIn().setBody( newList );
                newList.add( newExchange.getIn() );
                return oldExchange;
            }
            //in case it is n-th element where n > 1 I just add up content of the Message to existing list taken from aggregated Message
            List<Message> existingList = oldExchange.getIn().getBody( List.class );
            existingList.add( newExchange.getIn());
            return oldExchange;
        }
    }
    
    //class that sorts messages in batches of X per each clientId (whatever clientId is and however it is obtained - this is implementation specific and I can't know this)
    protected static class MessageSorter {
        
        public MessageSorter(List<Message> messages, int batchSize) {
            this.messages = messages;
            this.batchSize = batchSize;
        }
        
        public List<Message> sort() {
            
            //split messages per client
            TreeMap<String, List<Message>> messagesPerClientId = splitPerClient();

            //reorder messages so they are in batches of batchSize per client:
            //client1_batch1, client2_batch1, ... , clientN_batch1, client1_batch2, client2_batch_2, ... , client1_batch3 ... 
            List<Message> result = new ArrayList<>();
            //sorting ends when messages for all clients were distributed
            while (!messagesPerClientId.isEmpty()) {
                for (Iterator<Entry<String, List<Message>>> it = messagesPerClientId.entrySet().iterator();it.hasNext();) {
                    
                    List<Message> messagesForClient = it.next().getValue();
                    
                    //we have exhausted all messages for given client, so our batch  for this client will have less than batchSize elements
                    //we remove client from the sorting
                    if (messagesForClient.size() <= batchSize) {
                        it.remove();
                        result.addAll( messagesForClient );
                    }
                    //take 'batchSize' number of elements for given client and remove them from list of messages for client that remain
                    else {
                        List<Message> messagesToBatch = messagesForClient.subList( 0, batchSize );
                        result.addAll( messagesToBatch );
                        //NOTE: this will effectively remove elements we took from messagesForClient list
                        messagesToBatch.clear();
                    }
                }
            }
            
            return result;
        }

        //simple transformation that elements elements per client
        //would have been shorter if I could use lambdas... ;-)
        private TreeMap<String, List<Message>> splitPerClient() {
            TreeMap<String, List<Message>> messagesPerClientId = new TreeMap<>();
            for (Message message : messages) {
                String clientId = getClientId(message);
                //this is my dummy implementation to find out to which client message belongs
                //depending on where this info really is has to be done differently
                List<Message> messagesForClientId = messagesPerClientId.get( clientId );
                if ( null == messagesForClientId) {
                    messagesForClientId = new ArrayList<>();
                    messagesPerClientId.put( clientId, messagesForClientId );
                }
                messagesForClientId.add( message );
            }
            return messagesPerClientId;
        }
        
        //I assume here that I extract client id from file name, where files names are always:
        //data_{clientId}_{some_counter}.txt
        //important is to extract {clientId} - which I will use as identifier to which client file belongs
        //so I assume that info about to which client file belongs is in file name
        //if it is not so - then we need to extract this info from elsewhere (file path, file content - I can't say)
        private String getClientId(Message message) {
            return message.getHeader( "CamelFileName", String.class ).substring( 5, 7 );
        }
        
        private List<Message> messages;
        private int batchSize;
    }

}

