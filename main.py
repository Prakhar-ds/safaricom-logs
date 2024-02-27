import os
import sys
import pandas as pd
from boto3 import Session

boto_session = Session(
    aws_access_key_id=os.environ["aws_access_key_id"],
    aws_secret_access_key=os.environ["aws_secret_access_key"],
    region_name="ap-south-1"
)

client = boto_session.client('logs')

# Just replace it with your destination path
csv_folder_path = r'C:\Users\PrakharDeepSingh\Desktop\\'


def read_logs(logStreamName):
    print(logStreamName)
    try:
        log_events = client.get_log_events(
            logGroupName="safaricom-log-group",
            logStreamName=logStreamName,
        )
    except client.exceptions.ResourceNotFoundException as e:
        print("resource not found")
        return False
    except Exception as err:
        print(err)
    return log_events["events"]


if __name__ == '__main__':
    logStreamName = str(sys.argv[1])
    events = read_logs(logStreamName=logStreamName)
    if events:
        df = pd.DataFrame(events)
        df.drop(['ingestionTime'], axis=1, inplace=True)
        df["email"] = df["message"].apply(lambda message: message.split(";sep")[1])
        df["request_time"] = df["message"].apply(lambda message: message.split(";sep")[2])
        df["ip"] = df["message"].apply(lambda message: message.split(";sep")[3])
        df["message"] = df["message"].apply(lambda message: message.split(";sep")[0])
        print(df)
        df.to_csv(os.path.join(csv_folder_path + '{}.csv'.format(logStreamName)), index=False)
        print("logs file saved successfully!")
    else:
        print("The logs for input date does not exists")


