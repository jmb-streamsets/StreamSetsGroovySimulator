package src

import sdc.simulator.sdc.Record
import sdc.simulator.sdc.Sdc
import sdc.simulator.sdc.SdcSimulator

static void main(String[] args) {

/**
 * Create Pipeline Parameters
 **/
    Map pipelineParameters = [
            "param1": "value1",
            "param2": 42,
            "param3": true
    ]

/**
 * Create an instance object of the SdcSimulator
 **/
    def simulator = new SdcSimulator(pipelineParameters)

/**
 * Create an instance object of the RandomDateGenerator
 * It is returning a random date for the past or the future
 * Check the class to get more insight
 **/
    def randomDateGenerator = new RandomDateGenerator() // <---  helper function for the `simulator.createBatch`

    println "***************************************************"
    println " SDC Simulator for groovy code started version 0.1 "
    println "***************************************************"

    println "***************************************************"
    println " Step 0: Set the Init Script                       "
    println "***************************************************"

/**
 *
 * Copy the code in between this comment blocks then paste it to your groovy evaluator Init Script Box
 *
 * Remember to remove `simulator.` to have compatible code
 *
 * groovy evaluator Init Script Box -> Code Start*/

    simulator.sdc.state['my-state-variable'] = simulator.sdc.pipelineParameters()["param1"]

/**
 * groovy evaluator Init Script Box -> Code End
 **/


/**
 * simulator.createBatch start
 *
 * The `simulator.createBatch` closure is designed to generate a batch of data the same way
 **/
    println "***************************************************"
    println " Step 1: Generating data for the input batch       "
    println "***************************************************"
    simulator.createBatch(1) { Record record, int i ->

        record.value = Sdc.createMap(true)
        record.value['id'] = i
        record.value['firstName'] = 'John'
        record.value['lastName'] = 'Doe'
        record.value['created_on_ts'] = randomDateGenerator.generateRandomDate(Direction.PAST, 600)
        record.attributes['jdbc.tables'] = "person"
        record.attributes['jdbc.primaryKeySpecification'] = '{"id":{"type":4,"datatype":"INTEGER","size":11,"precision":10,"scale":0,"signed":true,"currency":false}}'
        record.attributes['jdbc.vendor'] = 'Snowflake'

    }
/**
 * simulator.createBatch end*/

    println "***************************************************"
    println " Step 2: Processing data from the input batch      "
    println "***************************************************"

    simulator.processBatch { Record record, Sdc sdc ->

/**
 * Copy and paste the code in between these comment blocks to the groovy evaluator script box.
 *
 * This code would be inserted in a closure like the one below to process every record received from upstream in the pipeline
 *
 *      sdc.records.each {  record ->
 *
 *          <---------paste code -------------->
 *
 *      }
 *
 * groovy evaluator Script Box -> Code Start
 * */
        try {

            // Change record root field value to a String value.
            // record.value = "Hello"
            // Change record root field value to a map value and create an entry
            // record.value = [firstName:'John', lastName:'Doe', age:25]

            record.value['pipelineParameter'] = sdc.pipelineParameters()['param1']

            // Access a map entry
            record.value['fullName'] = record.value['firstName'] + ' ' + record.value['lastName']

            // Create a list entry
            record.value['myList'] = [1, 2, 3, 4]

            // Modify an existing list entry
            ((List) record.value['myList'])[0] = 5

            // Assign a integer type to a field and value null
            record.value['null_int'] = sdc.NULL_INTEGER

            // Check if the field is NULL_INTEGER. If so, assign a value
            if (sdc.getFieldNull(record, '/null_int') == sdc.NULL_INTEGER) {
                record.value['null_int'] = 123
            }

            // Create a new record with map field
            def newRecord = sdc.createRecord(record.sourceId + ':newRecordId')
            newRecord.value = ['field1': 'val1', 'field2': 'val2']
            def newMap = sdc.createMap(true)
            newMap['field'] = 'val'
            newRecord.value['field2'] = newMap
            sdc.output.write(newRecord)

            // Modify a record header attribute entry
            record.attributes['name'] = record.attributes['first_name'] + ' ' + record.attributes['last_name']

            // copy sdc.state item to a record field
            record.value['sdc_state'] = simulator.sdc.state['my-state-variable']

            // Get a record header with field names ex. get sourceId and errorCode
            String sourceId = record.sourceId
            String errorCode = ''
            if (record.errorCode) {
                errorCode = record.errorCode
            }

            // Create an Event then send it to the event lane
            def evt = sdc.createEvent("unknown", 1)
            evt.value = sdc.createMap(true)
            evt.value['information'] = 'An unknown entity'
            evt.attributes['prop-01'] = '---prop-01---'
            sdc.toEvent(evt)

            sdc.output.write(record)
        } catch (Exception e) {
            sdc.log.error "Exception Message: $e.message" // Message of the exception
            sdc.log.error "Exception Cause: $e.cause" // Cause of the exception (can be null)
            sdc.log.error "Exception Class: $e.class.name" // Class name of the exception
            sdc.log.error "Exception Stack Trace:"
            e.stackTrace.each { sdc.log.error it as String } // Print each stack trace element
            sdc.error.write(record, "Something is not cool here...")
        }
/**
 * groovy evaluator Script Box -> Code End
 **/
    }

    println "***************************************************"
    println " Step 3: Listing data from the output batch        "
    println "***************************************************"
    simulator.outputBatch()

/**
 *
 * Copy the code in between this comment blocks then paste it to your groovy evaluator Destroy Script Box
 *
 * Remember to remove `simulator.` from all simulator.sdc.state[] state variable
 *
 * groovy evaluator Init Script Box -> Code Start
 * */
    simulator.sdc.state['logic-to-run'] = null
/**
 * groovy evaluator Destroy Script Box -> Code End*/

    System.exit(0)
}


