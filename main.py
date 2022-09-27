import datetime
import pytz
import psycopg2
import json
import schedule
import time
import boto3
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-2"
HOST = "aquaterradb.cpcnttzwogzc.ap-southeast-2.rds.amazonaws.com"
PORT = 5432
DATABASE = 'sdb_aquaterra'
USER = 'aquaTerra'
PASSWORD = 'Aquaterra88'


def send_email(username, inactive_sensors, mail_address):
    print('start sending email ...', flush=True)
    sensors = []
    text = ""
    for sensor, last_time in inactive_sensors:
        sensors.append(sensor)
        text += f"Sensor: {sensor} --- Last record time: {last_time} <br>"
    SENDER = "Aquaterra Notification <notifications@aquaterra.cloud>"
    RECIPIENT = mail_address
    # RECIPIENT = 'hongweih@student.unimelb.edu.au'
    sending_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SUBJECT = "No Data Sent Notification"
    BODY_HTML = f"""
    <p>
    <span style = "border-style: solid;"> There is an alert from Aquaterra. </span>
    <br><br>
    Hello, {username}
    <br><br>
    <span style = "color: #ba4920;"> Your sensor(s) has stopped sending data for a long period. </span>
    <br>
    {text}
    <br>
    This message is sent at: {sending_time}
    <br><br>
    Please contact AquaTerra on info@aquaterra.cloud for further instructions.
    </p>
    """
    CHARSET = "UTF-8"
    # legacy token
    ses_client = boto3.client('ses', region_name=AWS_REGION, aws_access_key_id="AKIAWJL2Z3QTPSJU6EO7",
                              aws_secret_access_key="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")
    try:
        # Provide the contents of the email.
        ses_response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': "",
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:", ses_response['MessageId']),

    print('mail has sent', flush=True)
    for sensor_id in sensors:
        set_notified(sensor_id)


def send_email_helper(send_email_dict):
    sensors = []
    for user, state in list(send_email_dict.items()):  # {'user': {}, }
        # print(type(user))
        # print('user', user)
        send_email(str(user), state['inactive_sensors'], state['email'])
        for sensor, last_time in state['inactive_sensors']:
            sensors.append(sensor)


def set_notified(sensor_id):
    conn = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=PASSWORD)
    with conn:
        query = f"UPDATE sensors SET has_notified = true WHERE sensor_id = '{sensor_id}';"
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    print("has notified", sensor_id, flush=True)


def get_email_address(send_email_dict):
    client = boto3.client('cognito-idp', region_name=AWS_REGION, aws_access_key_id="AKIAWJL2Z3QTPSJU6EO7",
                          aws_secret_access_key="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")
    user_data_response = client.list_users(UserPoolId="ap-southeast-2_cSiT8o4mI", )
    print('boto3 response:', user_data_response, flush=True)

    for user, state in list(send_email_dict.items()):
        for data in user_data_response['Users']:  # data: {'Username': str, 'Attributes': [{},...]}
            if user == data['Username']:
                print('user', user)
                for item in data['Attributes']:
                    print('item', item)
                    if item['Name'] == 'email':
                        state['email'] = item['Value']
                        break
    print('sending list', send_email_dict, flush=True)


def find_inactive_sensor(stopped_sensors, send_email_dict):
    conn = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=PASSWORD)
    with conn:
        query = "SELECT sensor_id, username FROM sensors WHERE is_active = false AND has_notified = false;"
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()  # all inactive sensor id
        conn.commit()
    for sensor_id, user in result:
        if user is not None:
            for id, time in stopped_sensors:
                if sensor_id == id:
                    if user not in send_email_dict.keys():
                        send_email_dict[user] = {'inactive_sensors': [(id, time.strftime("%Y-%m-%d %H:%M:%S"))],
                                                 'email': None}
                    else:
                        send_email_dict[user]['inactive_sensors'].append((id, time.strftime("%Y-%m-%d %H:%M:%S")))
    # print('stoped', stopped_sensors)


def check_sensor_working(stopped_sensors):
    conn = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=PASSWORD)
    with conn:
        query = "SELECT moisturedata.sensor_id, max(moisturedata.time) FROM moisturedata " \
                "GROUP BY moisturedata.sensor_id " \
                "ORDER BY max(moisturedata.time) DESC;"
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
        conn.commit()

    current_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    conn = psycopg2.connect(host=HOST, dbname=DATABASE, user=USER, password=PASSWORD)
    with conn:
        with conn.cursor() as cur:
            for sensor_id, data_time in result:
                time_difference = current_time - data_time
                if time_difference.total_seconds() > 86400:
                    # print('over_id', sensor_id, data_time)
                    query = f"UPDATE sensors SET is_active = false WHERE sensor_id = '{sensor_id}';"
                    cur.execute(query)
                    stopped_sensors.append((sensor_id, data_time))
                else:
                    # print('active_id', sensor_id)
                    query = f"UPDATE sensors SET is_active = true, has_notified = false WHERE sensor_id = '{sensor_id}';"
                    cur.execute(query)
        conn.commit()


def routine():
    stopped_sensors = []
    send_email_dict = {}  # {user1: {inactive_sensors: [(id, time), ...], email: None}, user2:...}
    check_sensor_working(stopped_sensors)
    print('--stopped sensor checked--', flush=True)
    find_inactive_sensor(stopped_sensors, send_email_dict)
    print('--inactive sensors found--', flush=True)
    get_email_address(send_email_dict)
    print('--got address--', flush=True)
    send_email_helper(send_email_dict)
    print('--sent email and updated database--', flush=True)
    present_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('Routine has finished at: ', present_time, flush=True)


if __name__ == '__main__':
    present_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Program runs at ", present_time, flush=True)
    print("Operating... will start routine every day at 00:00 ", flush=True)
    # routine()  # only for testing
    # comment out following when testing
    schedule.every().day.at("00:00").do(routine)
    while True:
        schedule.run_pending()
        time.sleep(1)
