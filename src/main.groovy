package src

import sdc.simulator.sdc.Record
import sdc.simulator.sdc.Sdc
import sdc.simulator.sdc.SdcSimulator

import java.security.MessageDigest
import java.util.concurrent.TimeUnit

static void main(String[] args) {
    //***************************************************************************
    // Step 0: Create the SDC simulator
    //***************************************************************************
    def simulator = new SdcSimulator()

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
 * Remember to remove `simulator.` from all simulator.sdc.state[] state variable
 *
 * To keep thing simple create sdc.state[] object to persist pipeline parameters like this :
 *
 *      sdc.state['logic-to-run'] = sdc.pipelineParameters()['logic']
 *
 * groovy evaluator Init Script Box -> Code Start*/

    simulator.sdc.state['logic-to-run'] = 'logic-1'
    // <---  In simulator just assigned your pipeline parameters this way

    MessageDigest md = MessageDigest.getInstance("MD5")

    def branchTable = [

            "logic-1": { record ->
                record.value['full_name'] = record.value['last_name'] + " " + record.value['first_name']
                md.update(((String) record.value['full_name']).bytes)
                byte[] digest = md.digest()
                record.value['digital_signature'] = digest.collect { String.format("%02x", it) }.join()
                record.value['updated_on_ts'] = new Date()
                return 200
            },

            "logic-2": { record ->
                record.value['array'] = ((String) record.value['full_name']).bytes
                return 200
            },

            "logic-3": { record ->
                record.value['tag'] = ((String) record.value['last_name'])[0..2] + '/' + ((String) record.value['first_name'])[-2..-1]
                return 200
            },

            "logic-4": { record ->
                record.value['delta_day'] = TimeUnit.MILLISECONDS.toDays(Math.abs(((Date) record.value['created_on_ts']).time - ((Date) record.value['updated_on_ts']).time))
                return 200
            },

            "default": { record -> return 404
            }]

    def cachedClosure = null
    def cachedKey = null

    def executeBranch = { key, record ->
        if (cachedKey != key) {
            // Cache the new closure if the key changes
            cachedKey = key
            cachedClosure = branchTable[key] ?: branchTable["default"]
        }
        // Call the cached closure
        return cachedClosure.call(record)
    }

    simulator.sdc.state['executeBranch'] = executeBranch

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
        record.value['first_name'] = 'John'
        record.value['last_name'] = 'Doe'
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

            record.value['status_1'] = sdc.state['executeBranch'](sdc.state['logic-to-run'], record)
            if (record.value['status_1'] != 404) {
                record.value['status_2'] = sdc.state['executeBranch']("logic-2", record)
                record.value['status_3'] = sdc.state['executeBranch']("logic-3", record)
                record.value['status_4'] = sdc.state['executeBranch']("logic-4", record)
            } else {
                def evt = sdc.createEvent("unknown", 1)
                evt.value = sdc.createMap(true)
                evt.value['information'] = 'An unknown Branch Table entry has been requested :' + sdc.state['logic-to-run']
                evt.attributes['prop-01'] = 'more detail here if needed'
                sdc.toEvent(evt)
            }

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
