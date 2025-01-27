# SteamSets Groovy Simulator

Provide StreamSets Groovy & Java programming and testing capability outside a StreamSets
pipeline evaluator stage

# SDC Simulator Groovy Script Workflow

---
![CapturedImage-23-01-2025 12-49-10.png](images/CapturedImage-23-01-2025%2012-49-10.png)

---
1. To start one can copy the `sdc-simulator-library-1.0-SNAPSHOT.jar` in the library path of an IDE
2. The [template.groovy](src/template.groovy) should be used to learn how to use the simulator
3. Use the StreamSets Data Collector documentation for the groovy evaluator
4. The API syntax of the simulator is identical to the one found in the StreamSets documentation

- Templates: 
  - [main.groovy](src/main.groovy)
  - [template.groovy](src/template.groovy)
  - [template_jdbc.groovy](src/template_jdbc.groovy)

---
# Init Script
-  
Like in the Groovy script box init section one would add any code logic that could be shared with the script & destroy box using a Data Collector state object 
![Screenshot from 2025-01-23 12-58-29.png](images/Screenshot%20from%202025-01-23%2012-58-29.png)

---
# Create input Batch
-
The `createBatch` closure is used to generate Data Collector record structure that will be used to test the code logic found in the `processBatch` closure 
![Screenshot from 2025-01-23 13-05-17.png](images/Screenshot%20from%202025-01-23%2013-05-17.png)

---
# Process input Batch
-
The `processBatch` closure logic is designed to run the code that will be placed inside the script box of the groovy evaluator  
![Screenshot from 2025-01-23 17-01-38.png](images/Screenshot%20from%202025-01-23%2017-01-38.png)












































