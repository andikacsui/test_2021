# test_2021

## Summary

1. On the `sql` folder, you can see the BigQery SQL that transformed the given tables based on the business problem.
2. On the `python_etl` folder, you can find the `salary_per_hour.py` ETL script designed based on the business problem.
3. On the `result` folder, you can find the ETL transformation results in CSV format from both SQL and python_etl.

## How to run the script

- Python 3 installation is needed, along with the pip-install for all the libraries in the `requirements.txt`
- To run the script, go to the python_etl directory, and write this command on the  line:
  python salary_per_hour.py <employees csv location> <timesheets csv location> <running date YYYY-MM-DD>.
  
  example:
  ```python
   python salary_per_hour.py employees.csv timesheets.csv 2021-01-05
  ```
- The detailed ETL rules are described on the short comments on the script :)
- As I used my personal GCP account, unfortunately I need to anonymize the service account

## Result

Both the runtime results from SQL and Python are identically the same, as the applied rules are exactly the same.

- For calculation rules: (total salary of all employees/total work hour of all employees), all is calculated based on the running month data
- Code parameterization: The scheduler/orchestrator can define the raw data location and running date when executing the script
- Idempotent: Multiple runs of the script for the same date will result the same in the end, as the existing results ot the running month will be overwritten if it already exists
- Deterministic: No random or unexact operation on the script. The result will be the same for multiple runtimes
