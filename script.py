import logging
import os
import sys
import warnings
from datetime import datetime, timedelta
from time import sleep

import requests
import schedule
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text

# Suppress specific warnings
warnings.filterwarnings(
    "ignore", message=".*The localize method is no longer necessary*"
)
warnings.filterwarnings(
    "ignore", message=".*The zone attribute is specific to pytz's interface*"
)

# Setup logger
logger = logging.getLogger("preekrooster")
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler("logfile.txt")

formatter = logging.Formatter("{asctime} - {levelname} - {message}", style="{")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Load environment variables
load_dotenv()


def load_env_variable(key, default=None):
    value = os.getenv(key)
    if not value and default is None:
        logger.error(f"Environment variable {key} is missing!")
        raise ValueError(f"Missing {key}")
    return value


# Database credentials
db_host = load_env_variable("DB_HOST")
db_user = load_env_variable("DB_USER")
db_password = load_env_variable("DB_PASSWORD")
db_name = load_env_variable("DB_NAME")
db_query = load_env_variable("DB_QUERY")

# Google API credentials
google_service_account_file = "./service_account.json"
google_calendar_id = load_env_variable("GOOGLE_CALENDAR_ID")

# Liturgy details
liturgy_url = load_env_variable("LITURGY_URL")


# Authenticate Google Calendar
def get_google_calendar_service():
    credentials = service_account.Credentials.from_service_account_file(
        google_service_account_file, scopes=["https://www.googleapis.com/auth/calendar"]
    )
    return build("calendar", "v3", credentials=credentials)


class Preekrooster:
    @staticmethod
    def create_google_calendar_event(row, existing_events):
        try:
            service = get_google_calendar_service()

            date = row[1]
            time = datetime.strptime(row[2].strip().replace(".", ":"), "%H:%M").time()

            datetime_start = datetime.combine(date, time)
            datetime_end = datetime_start + timedelta(hours=1.5)

            subject = row[3].strip().capitalize()
            predikant = row[4].strip()
            collecte1 = row[5].strip()
            collecte2 = row[6].strip()
            collecte3 = row[7].strip()
            location = "De Wijnstok"

            body = f"""
            <strong>Voorganger:</strong> {predikant}<br /><br />
            <strong>Collectedoelen:</strong><br />
            1. {collecte1}<br />
            2. {collecte2}<br />
            3. {collecte3}<br />
            <br />
            <strong><a href="https://www.youtube.com/@pkndubbeldam/live">Bekijk livestream</a></strong>
            """

            print("checking for events with start time:", datetime_start.isoformat())

            events = [
                event
                for event in existing_events
                if event["start"]["dateTime"][:19] == datetime_start.isoformat()[:19]
            ]

            if not events:
                Preekrooster.create_new_event(
                    service, subject, body, datetime_start, datetime_end, location
                )
            elif len(events) == 1:
                Preekrooster.update_existing_event(
                    service, events[0], subject, body, location, datetime_start
                )
            else:
                logger.warning("Multiple events found for the same time!")
                for event in events:
                    logger.warning(str(event))

        except Exception as e:
            logger.error(f"Error in creating or updating event: {str(e)}")

    @staticmethod
    def get_events_for_time_range(service):
        events_result = (
            service.events()
            .list(
                calendarId=google_calendar_id,
                singleEvents=True,
            )
            .execute()
        )
        return events_result.get("items", [])

    @staticmethod
    def create_new_event(service, subject, body, start, end, location):
        logger.debug("Creating new event")
        event = {
            "summary": subject,
            "location": location,
            "description": body,
            "start": {"dateTime": start.isoformat(), "timeZone": "Europe/Amsterdam"},
            "end": {"dateTime": end.isoformat(), "timeZone": "Europe/Amsterdam"},
        }
        event = (
            service.events().insert(calendarId=google_calendar_id, body=event).execute()
        )
        logger.debug(f"New event created: {event.get('htmlLink')}")

    @staticmethod
    def update_existing_event(service, event, subject, body, location, datetime_start):
        logger.info("Updating existing event")
        if Preekrooster.is_in_current_week(datetime_start):
            liturgie = Preekrooster.get_liturgie(datetime_start)
            if liturgie:
                body += """
                <br />
                <br />
                <strong><a href="https://www.pkndubbeldam.nl/files/liturgie.pdf">Druk hier voor de liturgie</a></strong>
                """
            else:
                body += """
                <br />
                <br />
                <strong>Liturgie nog niet beschikbaar</strong>
                """
        event["summary"] = subject
        event["description"] = body
        event["location"] = location
        updated_event = (
            service.events()
            .update(calendarId=google_calendar_id, eventId=event["id"], body=event)
            .execute()
        )
        logger.debug(f"Event updated: {updated_event.get('htmlLink')}")

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

            headers = {
                "If-Modified-Since": first_date_of_week.strftime(
                    "%a, %d %b %Y %H:%M:%S GMT"
                )
            }
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
            db_engine = create_engine(
                f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
            )
            with db_engine.connect() as connection:
                query = text(db_query)
                return connection.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Error fetching rows from database: {str(e)}")
            return []

    @staticmethod
    def clear_calendar():
        service = get_google_calendar_service()
        events = service.events().list(calendarId=google_calendar_id).execute()
        for event in events.get("items", []):
            service.events().delete(
                calendarId=google_calendar_id, eventId=event["id"]
            ).execute()

    @staticmethod
    def run():
        logger.debug("Running preekrooster job")
        database_rows = Preekrooster.get_rows_from_database()

        service = get_google_calendar_service()

        events = Preekrooster.get_events_for_time_range(service)

        for row in database_rows:
            Preekrooster.create_google_calendar_event(row, events)
        logger.debug("Preekrooster job completed")


def main():
    schedule.every().day.at("00:05").do(Preekrooster.run)
    schedule.every().sunday.at("09:30").do(Preekrooster.run)

    while True:
        schedule.run_pending()
        sleep(1)


if __name__ == "__main__":
    # Preekrooster.clear_calendar()
    Preekrooster.run()
    main()
