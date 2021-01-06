import sys
from datetime import datetime
import pandas as pd
from pandas_gbq import to_gbq, read_gbq
from google.oauth2 import service_account

class SalaryPerHour:

    def __init__(self):
        """
        Initiate instance attributes and validate the user's runtime parameters

        """

        try:
            self.employees = sys.argv[1]
            self.timesheets = sys.argv[2]
            self.date = datetime.strptime(sys.argv[3], '%Y-%m-%d').date()

            self.gcp_credentials = service_account.Credentials.from_service_account_file(
                'andika-coba-16bda8d2d448.json')

            self.query_deletion = """
                DELETE FROM `andika-coba.mekari.salary_per_hour_py`
                WHERE year = {year} AND month = {month}
            
            """

        except Exception as e:
            print('INPUT ERROR: Please ensure that the script is executed with following format:\n'
            'python salary_per_hour.py <employees csv location> <timesheets csv location> <running date YYYY-MM-DD>\n'
            'example: python salary_per_hour.py employees.csv timesheets.csv 2021-01-05')
            print('{}: {}'.format(type(e), str(e)))
            sys.exit(1)

    def extract(self):
        """
        Extract the data from CSV files and filter the timesheets data based on the input month.
        The timesheets data is filtered from the date 1 of the input date to the input date.

        :return:
        - df_employees: employees data frame
        - df_timesheets: filtered timesheets data frame
        """

        try:

            print('Extracting and filtering data')

            month_beginning_date = self.date.replace(day=1)

            df_employees = pd.read_csv(self.employees)
            df_timesheets = pd.read_csv(self.timesheets)

            df_timesheets = df_timesheets.loc[df_timesheets['date'] >= str(month_beginning_date)]
            df_timesheets = df_timesheets.loc[df_timesheets['date'] <= str(self.date)]

            return df_employees, df_timesheets

        except Exception as e:
            print('{}: {}'.format(type(e), str(e)))
            sys.exit(1)

    def transform(self, df_employees, df_timesheet):
        """
        Transform the extracted data based on the business definition

        :param df_employees: employees data frame
        :param df_timesheet: filtered timesheets data frame
        :return: final transformation result data frame
        """
        try:

            print('Transforming data')

            # Filter the rows with NULL values either in chekin or checkout - dirty data
            df_timesheet = df_timesheet.loc[df_timesheet['checkin'].notnull() & df_timesheet['checkout'].notnull()]

            # Remove the duplicates on the timesheet data frame
            df_timesheet = df_timesheet[['employee_id', 'date', 'checkin', 'checkout']].drop_duplicates()

            # Convert the checkin column to datetime to be calculated
            df_timesheet['checkin'] = df_timesheet.apply(
                lambda x: datetime.strptime('{} {}'.format(x['date'], x['checkin']), '%Y-%m-%d %H:%M:%S'),
                axis=1
            )

            # Convert the checkout column to datetime to be calculated
            df_timesheet['checkout'] = df_timesheet.apply(
                lambda x: datetime.strptime('{} {}'.format(x['date'], x['checkout']), '%Y-%m-%d %H:%M:%S'),
                axis=1
            )

            # Only proceed the data if checkin time <= checkout time
            # other data are considered as dirty data
            df_timesheet = df_timesheet.loc[df_timesheet['checkin'] <= df_timesheet['checkout']]

            # Measure the working hour of the day of an employee, the result is floored
            df_timesheet['work_hour'] = df_timesheet.apply(
                lambda x: (x['checkout'] - x['checkin']).seconds//3600,
                axis=1
            )

            # Remove the duplicated employees entries, if the salary is difference, take the maximum one
            df_employees = df_employees.groupby(['employee_id', 'branch_id'], as_index=False)['salary'].max()

            # Merge the timesheet and employees data frames
            df_merge = pd.merge(df_timesheet, df_employees, how='left', on='employee_id')

            # Perform group by to get the summary of employee work hour in a branch on that month
            df_merge = df_merge.groupby(['branch_id', 'employee_id'], as_index=False).agg(
                {
                    'work_hour':'sum',
                    'salary':'max',
                }
            )

            # Filter out if the work hour is less than or equal to 0, as it will spoil the calculation
            df_merge = df_merge.loc[df_merge['work_hour'] > 0]

            # Perform group by to sum the total work hour and salary of all employees in a branch on the month
            df_merge = df_merge.groupby(['branch_id'], as_index=False).agg(
                {
                    'work_hour': 'sum',
                    'salary': 'sum'
                }
            )

            # Calculate the salary_per_hour
            df_merge['salary_per_hour'] = df_merge.apply(
                lambda x: int(x['salary'] / x['work_hour']),
                axis=1
            )

            # Addming year and month columns
            df_merge['year'] = self.date.year
            df_merge['month'] = self.date.month

            # Re-arrange the data frame columns
            df_result = df_merge[['year', 'month', 'branch_id', 'salary_per_hour']]

            return df_result

        except Exception as e:
            print('{}: {}'.format(type(e), str(e)))
            sys.exit(1)

    def load(self, df_result):
        """
        Load the transformation result to BigQuery

        :param df_result: final transformation result data frame
        """

        try:

            # Delete the same-month records if they are already there
            print('Deleting existing running month data to be replaced')
            read_gbq(
                self.query_deletion.format(year=self.date.year, month=self.date.month),
                project_id='andika-coba',
                credentials=self.gcp_credentials,
                dialect='standard'
            )

        except Exception as e:
            print('Table is not exist. Will create one.')

        # Load the transformation result to the BigQuery
        print('Appending the ETL result to the table')
        to_gbq(
            df_result,
            destination_table='mekari.salary_per_hour_py',
            project_id='andika-coba',
            credentials=self.gcp_credentials,
            chunksize=20000,
            if_exists='append'
        )


sph = SalaryPerHour()
df_employees, df_timesheet = sph.extract()
df_result = sph.transform(df_employees, df_timesheet)
sph.load(df_result)
