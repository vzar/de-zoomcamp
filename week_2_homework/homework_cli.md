# deployment CLI commands

# Question 2
# % prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs -n etl_cron --cron "0 5 1 * *" 

# Question 4
# % prefect deployment build -n etl_github -sb github/github-prefect-storage -o week_2_homework/github_flow.yaml ./week_2_homework/flows/etl_web_to_gcs.py:etl_web_to_gcs 
# % prefect deployment apply week_2_homework/github_flow.yaml

