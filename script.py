import os
from time import sleep
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from O365 import Account, MSGraphProtocol
import re
import warnings
import logging
import schedule
import requests
import base64
from bs4 import BeautifulSoup
import pypdfium2 as pdfium
import PIL

warnings.filterwarnings("ignore", message=".*The localize method is no longer necessary*")
warnings.filterwarnings("ignore", message=".*The zone attribute is specific to pytz's interface*")

# Setup logger

logging.basicConfig(filename="log.txt",
                    filemode='a',
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Load environment variables from .env file
load_dotenv()

# Get database connection details from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_table = os.getenv('DB_TABLE')
db_query = os.getenv('DB_QUERY')

# Get Azure / app credentials from environment variables
azure_client_id = os.getenv('AZURE_CLIENT_ID')
azure_secret_id = os.getenv('AZURE_SECRET_ID')
azure_tenant_id = os.getenv('AZURE_TENANT_ID')

# Get Outlook details from environment variables
outlook_email = os.getenv('OUTLOOK_EMAIL')
outlook_calendar_name = os.getenv('OUTLOOK_CALENDAR_NAME')

# Get Liturgie details from environment variables
liturgy_url = os.getenv('LITURGY_URL')

# Get Wordpress details from environment variables
wordpress_url = os.getenv('WORDPRESS_URL')
wordpress_user = os.getenv('WORDPRESS_USER')
wordpress_password = os.getenv('WORDPRESS_PASSWORD')

# Get Youtube details
youtube_channel_id = os.getenv('YOUTUBE_CHANNEL_ID')

# Set up Outlook connection
protocol = MSGraphProtocol(api_version='v1.0')

credentials = (azure_client_id, azure_secret_id)
account = Account(credentials, auth_flow_type='credentials', tenant_id=azure_tenant_id, protocol=protocol)

class preekrooster:
    def create_outlook_event(row):
        schedule = account.schedule(resource=outlook_email)
        calendar = schedule.get_calendar(calendar_name=outlook_calendar_name)

        date = row[1]
        time = datetime.strptime(row[2].strip().replace(".", ":"), '%H:%M').time()

        datetime_start = datetime.combine(date,time)
        datetime_end = datetime_start + timedelta(hours=1.5)

        subject = row[3].strip().capitalize()
        predikant = row[4].strip()
        collecte1 = row[5].strip()
        collecte2 = row[6].strip()
        collecte3 = row[7].strip()
        location = 'De Wijnstok'

        body = "&lt;b&gt;Voorganger:&lt;/b&gt;&lt;br&gt;" + predikant + "&lt;br&gt;&lt;br&gt;&lt;b&gt;Collectedoelen:&lt;/b&gt;" + "&lt;br&gt;1. " + collecte1 + "&lt;br&gt;2. " + collecte2 + "&lt;br&gt;3. " + collecte3 + "&lt;br&gt;&lt;br&gt; &lt;b&gt;&lt;a href='https://www.youtube.com/@pkndubbeldam/live'&gt;Bekijk livestream&lt;/a&gt;&lt;/b&gt;"

        q = calendar.new_query('start').greater_equal(datetime_start)
        q.chain('and').on_attribute('end').less_equal(datetime_end)

        events = calendar.get_events(query=q, include_recurring=True)
        event_list = list(events)
        event_count = len(event_list)

        if (event_count == 0):
            logger.info("Creating event:")

            new_event = calendar.new_event()

            if (preekrooster.is_in_current_week(datetime_start)):
                liturgie = preekrooster.get_liturgie(datetime_start)
                if (liturgie):
                    body += "&lt;br&gt;&lt;br&gt; &lt;b&gt;&lt;a href='https://www.pkndubbeldam.nl/files/liturgie.pdf'&gt;Druk hier voor de liturgie&lt;/a&gt;&lt;/b&gt;"
                else:
                    body += "&lt;br&gt;&lt;br&gt; &lt;b&gt;&lt;a&gt;Liturgie nog niet beschikbaar&lt;/a&gt;&lt;/b&gt;"
                # new_event.attachments.remove('Liturgie.pdf')
                # new_event.attachments.add('Liturgie.pdf')
                
                logger.info("ADDED LITURGY")

            new_event.subject = subject
            new_event.body = body
            new_event.location = location
            new_event.start = datetime_start
            new_event.end = datetime_end

            new_event.save()
            logger.info(str(new_event) + "\n")
        elif (event_count == 1):
            logger.info("Replacing event:")

            event = event_list[0]

            if (preekrooster.is_in_current_week(datetime_start)):
                liturgie = preekrooster.get_liturgie(datetime_start)
                if (liturgie):
                    body += "&lt;br&gt;&lt;br&gt; &lt;b&gt;&lt;a href='https://www.pkndubbeldam.nl/files/liturgie.pdf'&gt;Druk hier voor de liturgie&lt;/a&gt;&lt;/b&gt;"
                else:
                    body += "&lt;br&gt;&lt;br&gt; &lt;b&gt;&lt;a&gt;Liturgie nog niet beschikbaar&lt;/a&gt;&lt;/b&gt;"
                # event.attachments.remove('Liturgie.pdf')
                # event.attachments.add('Liturgie.pdf')
                logger.info("ADDED LITURGY")

            event.subject = subject
            event.body = body
            event.location = location

            event.save()
            logger.info(str(event) + "\n")
        else:
            logger.warning("Two or more events at the same time:")
            for event in event_list:
                logger.warning(str(event))

        # sleep(2)

    def get_rows_from_database():
        # Set up database connection
        db_engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")\

        # Check the database for new rows
        with db_engine.connect() as connection:

            query = text(db_query)
            rows = connection.execute(query)

            return rows

    def is_in_current_week(date):
        current_week, current_year, _ = datetime.now().isocalendar()

        event_week, event_year, _ = date.isocalendar()

        return current_week == event_week and current_year == event_year

    def get_liturgie(event_date):
        # Calculate the number of days to subtract to get to the Monday of the current week
        days_to_subtract = event_date.weekday()

        # Calculate the first date (Monday) of the current week with a time of 00:00
        first_date_of_week = event_date - timedelta(days=days_to_subtract, hours=event_date.hour, minutes=event_date.minute, seconds=event_date.second, microseconds=event_date.microsecond)

        headers = {
            'If-Modified-Since': first_date_of_week.strftime('%a, %d %b %Y %H:%M:%S GMT')
        }

        response = requests.get(liturgy_url, headers=headers)

        if response.status_code == 200:
            return True

        elif response.status_code == 304:
            logger.info("Liturgy not from this week!")
            return False
        else:
            logger.debug("An error occurred while retrieving the liturgy.")
            return False

    def run():
        logger.debug("Import started\n")
        
        database_rows = preekrooster.get_rows_from_database()

        account.authenticate()

        for row in database_rows:
            preekrooster.create_outlook_event(row)

        logger.debug("Import done\n")

        os.remove('o365_token.txt')

def main(): 
    logger.debug("Script started\n")
    schedule.every().day.at("00:05").do(preekrooster.run)
    
    schedule.every().sunday.at("09:30").do(preekrooster.run)

    while True:
        schedule.run_pending()
        sleep(1)

if __name__ == '__main__':
    preekrooster.run()
    main()




#class liturgie_media:
    # def run():
    #     liturgie_exists = liturgie_media.get_liturgie()

    #     if (liturgie_exists):
            
    #         content = """
    #             <a>De liturgie voor aankomende zondag is beschikbaar.</a>
    #             <br>
    #             <br>
    #             <a>https://www.pkndubbeldam.nl/files/liturgie.pdf</a>
    #         """

    #         content = content.replace("<", "&lt;")
    #         content = content.replace(">", "&gt;")
        
    #         date = liturgie_media.get_next_sunday()

    #         wordpress.create_wordpress_post(
    #             f"Liturgie {date}",
    #             content,
    #             get_category_by_name("Liturgie & livestream")
    #         )

    # def get_next_sunday():
    #     current_date = datetime.now()
    #     days_until_sunday = (6 - current_date.weekday() + 7) % 7
    #     next_sunday = current_date + timedelta(days=days_until_sunday)
    #     return next_sunday.strftime("%d-%m-%Y")

    # def get_liturgie(event_date):
    #     if (event_date != None):
    #         # Calculate the number of days to subtract to get to the Monday of the current week
    #         days_to_subtract = event_date.weekday()

    #         # Calculate the first date (Monday) of the current week with a time of 00:00
    #         first_date_of_week = event_date - timedelta(days=days_to_subtract, hours=event_date.hour, minutes=event_date.minute, seconds=event_date.second, microseconds=event_date.microsecond)
    #     else:
    #         # Calculate the number of days to subtract to get to the Monday of the current week
    #         time = datetime.now()
    #         days_to_subtract = time.weekday()

    #         # Calculate the first date (Monday) of the current week with a time of 00:00
    #         first_date_of_week = time - timedelta(days=days_to_subtract-4, hours=time.hour, minutes=time.minute, seconds=time.second, microseconds=time.microsecond)
            
    #     headers = {
    #         'If-Modified-Since': first_date_of_week.strftime('%a, %d %b %Y %H:%M:%S GMT')
    #     }

    #     response = requests.get(liturgy_url, headers=headers)

    #     if response.status_code == 200:
    #         content = response.content
    #         # with open('Liturgie.pdf', 'wb') as file:
    #             # file.write(content)
    #         pdf = pdfium.PdfDocument(content)
    #         page = pdf[0]
    #         pil_image = page.render(scale=4).to_pil()
    #         pil_image.save("output.jpg")
    #         return True

    #     elif response.status_code == 304:
    #         logger.info("Liturgy not from this week!")
    #         return False
    #     else:
    #         logger.debug("An error occurred while retrieving the liturgy.")
    #         return False

#class wordpress:
    # wordpress_credentials = f"{wordpress_user}:{wordpress_password}"
    # wordpress_token = base64.b64encode(wordpress_credentials.encode())
    # wordpress_header = {'Authorization': 'Basic ' + wordpress_token.decode('utf-8')}

    # def create_wordpress_post(title, content, category):
    #     api_url = wordpress_url + '/wp-json/wp/v2/posts'
    #     data = {
    #         'title' : title,
    #         'status': 'publish',
    #         'categories': [category],
    #         'content': content
    #     }
    #     response = requests.post(api_url, headers=wordpress_header, json=data)
        
    #     if (response.status_code == 201):
    #         print("Published new post!")
    #     else:
    #         print("There wen't something wrong!")
    #         print(response.text)

    # def check_if_already_posted():
    #     api_url = wordpress_url + '/wp-json/wp/v2/posts'
    #     response = requests.get(api_url, headers=wordpress_header)
        
    #     if (response.status_code == 201):
    #         print("Published new post!")
    #     else:
    #         print("There wen't something wrong!")
    #         print(response.text)

    # def get_category_by_name(category_name):
    #     if category_name == "App":
    #         return 43
    #     elif category_name == "Liturgie & livestream":
    #         return 44
    #     else:
    #         return None
