Clone the repository to a cloud plattform because analysis 3 of the application contains a logistic regression model.
Make sure to upload the files: rewards_receipts_item_lat_v2.csv and rewards_receipts_lat_v3.csv to the cloud platform as well.
Under the def main function: there will be 3 variables: analysis, month, and format that you can assign different values. 
the variable (analysis) takes a string and has a default value of "analysis1", if you want to run analysis 2 instead change the value to "analysis2", or to run analysis 3 change the value to "analysis3".
The variable (format) takes a string and has a default value of "csv" that is used in analysis 2, if you want to return instead a JSON foramt, change the default value to "JSON", or if you want to return a parquet format chnage the value to "Parquet".
the variable (month) takes an Int and is set to a default value of 2. You change the value to any integer between 1 and 12. (Do not include a zero in front of numbers less than 10).
Run the Application.
