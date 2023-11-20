import logging
from pyspark.sql import SparkSession

logging.basicConfig(filename='spark_job.log', level=logging.INFO)


spark = SparkSession.builder.getOrCreate()


from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])


df.collect()


df.filter(df.a == 1).show()





@app.route("/api/fici", methods = ['POST', 'GET'])
def fici():
    if request.method == "POST":
            file_path = pathlib.Path('fici.json')
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                # Get the JSON data from the request
            data = request.data

            # Check if there is any data
            if not data:
                return jsonify({'error': 'No data provided'}), 400

            try:
                # Parse the JSON data
                json_data = data.decode('utf-8')  # Decode the bytes to a string
                data_dict = json.loads(json_data)
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid JSON data'}), 400

            # Access the data in the dictionary
            eventName = data_dict.get('eventName')
            eventDescription = data_dict.get('eventDescription')
            startDate = data_dict.get('startDate')
            endDate = data_dict.get('endDate')

            if eventName is None or eventDescription is None or startDate is None or endDate is None:
                return 
            else:
                # Now you can use the values as needed
                # For example, you can return them as a JSON response
                response_data = {
                    'eventName': eventName,
                    'eventDescription': eventDescription,
                    'startDate': startDate,
                    'endDate': endDate
                }

                # Add the item to the list of items
                existing_data.append(response_data)
                # Write the updated list back to the file
                with open(file_path, 'w') as file:
                    json.dump(existing_data, file, indent=4)
                # return jsonify(existing_data)
                # Prepare the response

            
                response_data = {
                    'status': 'success'
                }
                return jsonify([response_data])
    else:
        file_path = pathlib.Path('fici.json')
        with open(file_path, 'r') as file:
            existing_data = json.load(file)
        return jsonify({'events':existing_data})
        