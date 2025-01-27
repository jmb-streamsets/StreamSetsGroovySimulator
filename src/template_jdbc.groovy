package src


import sdc.simulator.sdc.Record
import sdc.simulator.sdc.Sdc
import sdc.simulator.sdc.SdcSimulator

import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.ResultSetMetaData

static void main(String[] args) {

/**
 * Create Pipeline Parameters
 **/
    Map pipelineParameters = [
            "capitalize": true,
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

    simulator.sdc.state['list'] = ['item1', 'item2']

/**
 * groovy evaluator Init Script Box -> Code End
 **/

    println "***************************************************"
    println " Step 1: Generating data for the input batch       "
    println "***************************************************"
    int max_records = 1000

/**
 * Custom code to create a data source that will be used by the simulator.createBatch
 *
 **/
    def dbUrl = 'jdbc:sqlserver://172.16.0.178:1433;Encrypt=False;TrustServerCertificate=True;databaseName=STREAMSETS'
    def dbUser = 'sa'
    def dbPassword = 'DuufhNG9188'
    def dbDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    def dbSchema = 'dbo'
    def dbTable = 'ComplexPKTable'

    Class.forName(dbDriver)
    def connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
    def statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    def query = "SELECT TOP ($max_records) * FROM ${dbSchema}.${dbTable} ORDER BY 1"
    def resultSet = statement.executeQuery(query)
    ResultSetMetaData metaData = resultSet.getMetaData()
    int columnCount = metaData.getColumnCount()

    /**
     * simulator.createBatch start
     *
     * The `simulator.createBatch` closure is designed to generate a batch of data the same way
     **/
    simulator.createBatch(max_records) { Record record, int i ->
        record.value = Sdc.createMap(true)

        /**
         * Read a record of the Query result set then duplicate all the column of the record to the new SDC record structure
         **/
        resultSet.next()
        (1..columnCount).each { index ->
            def columnName = metaData.getColumnLabel(index)
            def value = resultSet.getObject(index)
            record.value[columnName] = value
        }

        /**
         * Add many attributes as needed for the use case         *
         **/
        record.attributes['jdbc.tables'] = "ComplexPKTable"
        record.attributes['jdbc.primaryKeySpecification'] = '{"id":{"type":4,"datatype":"INTEGER","size":11,"precision":10,"scale":0,"signed":true,"currency":false}}'
        record.attributes['jdbc.vendor'] = 'Microsoft'
    }

    statement.close()
    connection.close()

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
            if (sdc.pipelineParameters()['capitalize']) {
                record.value.each { String name, Object value ->
                    record.value[name] = ((String) record.value[name]).toUpperCase()
                }
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


