from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

rulesDrop = {
  "valid_directory": "source_file LIKE '/Volumes/workspace/projects/iot_devices/iot_telemetry/%'",
  "timestamp_positive": "timestamp_us >= 0"
}

rulesWarn = {
  "temp_high_low": "temperatura > -20 and temperatura < 60",
  "valid_humidity_range": "humedad <= 100 and humedad >= 0"
}

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None




    