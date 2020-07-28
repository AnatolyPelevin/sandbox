####BigData Project Template
Is used to quickly create new project based on commonly needed functionality
Consists of 3 modules
* deployment - module with common deployment scenarios, databases initialization and configurations
* oozie-apps - module consists of a single oozie coordinator 
* template-app - module with empty Spark application run on oozie


How to use this template:
* Copy template project into the destination folder
* Check and change project **groupId**, **artifactId**, **project name** and application **module name** at all maven *pom.xml* files
* Change **package** names for oozie-apps and app modules according to the new **groupId** of the project. (Don't forget about test packages as well)
* Find and change oozie workflow name. It's used in *workflow.xml* and *.yaml* files. Currently it may be found by **workflow-template** pattern
* Change workflow **package** name and **.properties** file name
* Change **internal_vars.yaml** properties by appropriate values. Variable *projectName* is used in deploy scenarios as a folder name corresponding to the current project
* External variables are used as mocks for appropriate external memory settings and Vertica credentials. So it's enough to change its names at the places of its usage. See example in the *internal_vars.yaml*.
  * BIGDATA_PROJECT_TEMPLATE_MEMORY
  * TEMPLATE_VERTICA_USER_NAME
  * TEMPLATE_VERTICA_USER_PASSWORD
