# Quick start guide
Need to enable prod vpn.
1. Clone project
2. Set next variables:
    - config.json : "user"
    - config.json : "path"
    - passwords.json : "sf_prod_password"
    - passwords.json : "sf_lab_password"
    - passwords.json : "pg_password"
    - passwords.json : "password"
3. Fill new_fields.txt and new_formula_fields.txt like in example or leave them empty.
4. Run ./start_gen.sh

# Configuration
## Config.json

1. SalesForce Prod connection
    - "sf_prod_username" : username of prod sf account
    - "sf_prod_security_token" : security token of prod sf account
    - "sf_prod_domain" : domain of prod sf connection url


2. SalesForce Lab connection
    - "sf_lab_username" : username of lab sf account
    - "sf_lab_security_token" : security token of lab sf account
    - "sf_lab_domain" : domain of lab sf connection url
	
	
3. Heroku Prod connection
    - "ssh_host" : ssh host for ssh tunel
    - "user" : username for ssh tunel
    - "pg_host" : pg host on prod
    - "pg_database" : pg database name on prod
    - "pg_user": pg username on prod
	
	
4. Run variables
    - "fields_file_name" : name of file with new fields
    - "formula_fields_file_name" : name of file with new formula fields
    - "report_file_name" : name of file with report
    - "out_path" : path to directory with reports
    - "path" : absolute path to directory with json config files of sfdc-config
    - "change_object_config" : "true" means that files of sfdc-config was changed, "false" - just prepare report
    - "attempts" : reconnection attempts
    - "create_schema_files" : "true" - generator will write schema of object from source database to out_path directory
    - "report_columns" : contains name of report columns, mark "true" to columns, which you want to see at report

## Passwords.json
1. SalesForce password
    - "sf_prod_password" : salesforce prod account password
    - "sf_lab_password" : salesforce lab account password
    
    
2. Heroku password
    - "pg_password" : password of prod heroku database
    
    
3. User password
    - "password" : user password of production account, it is necessary for ssh tunel


# Additional information

For now, if we want to have different source and destination name, we should change it manually.

In case of troubles connect with Semyon.Putnikov.

For supported features and known issues look at wiki page: 
https://wiki.ringcentral.com/display/DATA/SFDC+ETL+config+-+SFDC+config+generator
