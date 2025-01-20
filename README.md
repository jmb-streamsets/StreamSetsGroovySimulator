# SteamSetsGroovySimulator

Provide StreamSets programming and testing capability outside a StreamSets
pipeline evaluator stage



# SDC Simulator Groovy Script Workflow

_A Visual Guide to Data Simulation and Processing_

---

## 1. Initialization

- **Initialize SdcSimulator**
    - Creates an instance of the `SdcSimulator`.

- **Initialize RandomDateGenerator**
    - Sets up the `RandomDateGenerator` for generating random dates.

- **Print Startup Messages**
    - Outputs messages indicating the simulator has started.

- **Init Script Setup**
    - Sets initial state variables.
  ```groovy
  sdc.state['my-state-variable'] = 'from-pipeline-parameters'



+-------------------------------------------------------------------------------------+
|                               SDC Simulator Workflow                                |
|                        A Visual Guide to Groovy Script Processing                   |
+-------------------------------------------------------------------------------------+
| [Initialization] --> [Batch Creation] --> [Batch Processing] --> [Output & Cleanup]  |
|                                                                                     |
| Initialization                                                                       |
| - Initialize SdcSimulator                                                            |
| - Initialize RandomDateGenerator                                                    |
| - Print Startup Messages                                                             |
| - Set Init Script State                                                              |
|                                                                                     |
| Batch Creation                                                                       |
| - Create Batch with 1 Record                                                         |
| - Populate Record Fields                                                             |
| - Assign JDBC Attributes                                                             |
|                                                                                     |
| Batch Processing                                                                     |
| - Iterate Over Each Record                                                           |
|   - Modify Fields (fullName, myList, null_int)                                      |
|   - Create New Record and Event                                                      |
|   - Update Record Attributes                                                         |
|   - Handle Errors and Logging                                                       |
|                                                                                     |
| Output & Cleanup                                                                     |
| - Display Processed Records                                                          |
| - Execute Destroy Script (Cleanup State)                                            |
| - Terminate Program                                                                  |
+-------------------------------------------------------------------------------------+



