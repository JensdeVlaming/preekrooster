import os
from time import sleep
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from O365 import Account, MSGraphProtocol
import warnings
import logging
import schedule
import requests
import sys 

# Suppress specific warnings
warnings.filterwarnings("ignore", message=".*The localize method is no longer necessary*")
warnings.filterwarnings("ignore", message=".*The zone attribute is specific to pytz's interface*")

# Setup logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all messages

# Create handlers for both console and file
console_handler = logging.StreamHandler(sys.stdout) # Logs to the stdout
file_handler = logging.FileHandler('logfile.txt')  # Logs to a file

# Set the log level for each handler (can be different)
console_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.DEBUG)

# Create formatters and add them to the handlers
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Load environment variables from .env file
load_dotenv()

def load_env_variable(key, default=None):
    value = os.getenv(key)
    if not value and default is None:
        logger.error(f"Environment variable {key} is missing!")
        raise ValueError(f"Missing {key}")
    return value

# Get database connection details
db_host = load_env_variable('DB_HOST')
db_user = load_env_variable('DB_USER')
db_password = load_env_variable('DB_PASSWORD')
db_name = load_env_variable('DB_NAME')
db_query = load_env_variable('DB_QUERY')

# Get Azure / app credentials
azure_client_id = load_env_variable('AZURE_CLIENT_ID')
azure_secret_id = load_env_variable('AZURE_SECRET_ID')
azure_tenant_id = load_env_variable('AZURE_TENANT_ID')

# Get Outlook details
outlook_email = load_env_variable('OUTLOOK_EMAIL')
outlook_calendar_name = load_env_variable('OUTLOOK_CALENDAR_NAME')

# Get Liturgie details
liturgy_url = load_env_variable('LITURGY_URL')

# Initialize Outlook Account
protocol = MSGraphProtocol(api_version='v1.0')
credentials = (azure_client_id, azure_secret_id)
account = Account(credentials, auth_flow_type='credentials', tenant_id=azure_tenant_id, protocol=protocol)

class Preekrooster:
    
    @staticmethod
    def create_outlook_event(row):
        try:
            schedule = account.schedule(resource=outlook_email)
            calendar = schedule.get_calendar(calendar_name=outlook_calendar_name)

            date = row[1]
            time = datetime.strptime(row[2].strip().replace(".", ":"), '%H:%M').time()

            datetime_start = datetime.combine(date, time)
            datetime_end = datetime_start + timedelta(hours=1.5)

            subject = row[3].strip().capitalize()
            predikant = row[4].strip()
            collecte1 = row[5].strip()
            collecte2 = row[6].strip()
            collecte3 = row[7].strip()
            location = 'De Wijnstok'

            body = f"""
            **Voorganger:**  
            {predikant}  

            **Collectedoelen:**  
            1. {collecte1}  
            2. {collecte2}  
            3. {collecte3}  

            **[Bekijk livestream](https://www.youtube.com/@pkndubbeldam/live)**
            """

            # Check for existing events in the given time range
            events = Preekrooster.get_events_for_time_range(calendar, datetime_start, datetime_end)

            if not events:
                Preekrooster.create_new_event(calendar, subject, body, datetime_start, datetime_end, location)
            elif len(events) == 1:
                Preekrooster.update_existing_event(events[0], subject, body, location, datetime_start)
            else:
                logger.warning("Multiple events found for the same time!")
                for event in events:
                    logger.warning(str(event))
        except Exception as e:
            logger.error(f"Error in creating or updating event: {str(e)}")

    @staticmethod
    def get_events_for_time_range(calendar, start, end):
        query = calendar.new_query('start').greater_equal(start)
        query.chain('and').on_attribute('end').less_equal(end)
        events = calendar.get_events(query=query, include_recurring=True)
        return list(events)

    @staticmethod
    def create_new_event(calendar, subject, body, start, end, location):
        logger.info("Creating new event")
        new_event = calendar.new_event()
        new_event.subject = subject
        new_event.body = body
        new_event.start = start
        new_event.end = end
        new_event.location = location
        new_event.save()
        logger.info(f"New event created: {new_event}")

    @staticmethod
    def update_existing_event(event, subject, body, location, datetime_start):
        logger.info("Updating existing event")
        if Preekrooster.is_in_current_week(datetime_start):
            liturgie = Preekrooster.get_liturgie(datetime_start)
            if liturgie:
                body += """  
                **[Druk hier voor de liturgie](https://www.pkndubbeldam.nl/files/liturgie.pdf)**  
                """
            else:
                body += """ 
                **Liturgie nog niet beschikbaar**  
                """

        event.subject = subject
        event.body = body
        event.location = location
        event.save()
        logger.info(f"Event updated: {event}")

    @staticmethod
    def is_in_current_week(date):
        current_week, current_year = datetime.now().isocalendar()[:2]
        event_week, event_year = date.isocalendar()[:2]
        return current_week == event_week and current_year == event_year

    @staticmethod
    def get_liturgie(event_date):
        try:
            days_to_subtract = event_date.weekday()
            first_date_of_week = event_date - timedelta(days=days_to_subtract)

            headers = {'If-Modified-Since': first_date_of_week.strftime('%a, %d %b %Y %H:%M:%S GMT')}
            response = requests.get(liturgy_url, headers=headers)

            if response.status_code == 200:
                logger.info("Liturgy available")
                return True
            elif response.status_code == 304:
                logger.info("Liturgy not available this week")
                return False
            else:
                logger.error("Error fetching liturgy")
                return False
        except Exception as e:
            logger.error(f"Error fetching liturgy: {str(e)}")
            return False

    @staticmethod
    def get_rows_from_database():
        try:
            db_engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
            with db_engine.connect() as connection:
                query = text(db_query)
                rows = connection.execute(query)
                return rows
        except Exception as e:
            logger.error(f"Error fetching rows from database: {str(e)}")
            return []

    @staticmethod
    def run():
        logger.info("Running preekrooster job")
        database_rows = Preekrooster.get_rows_from_database()
        account.authenticate()

        for row in database_rows:
            Preekrooster.create_outlook_event(row)
        logger.info("Preekrooster job completed")

def main():
    logger.info("Starting script")
    schedule.every().day.at("00:05").do(Preekrooster.run)
    schedule.every().sunday.at("09:30").do(Preekrooster.run)

    while True:
        schedule.run_pending()
        sleep(1)

if __name__ == '__main__':
    Preekrooster.run()
    main()
