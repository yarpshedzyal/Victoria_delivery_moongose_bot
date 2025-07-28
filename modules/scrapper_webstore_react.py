from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from modules.config_chrome import CHROME_OPTIONS, CHROME_SERVICE
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import re
import time



def create_driver():
    # Initialize the WebDriver with the configuration details
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=CHROME_OPTIONS)
    return driver

def extract_status(text):
    pattern = r'<span class="[^"]*order-status[^"]*">\s*(.*?)\s*</span>'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    else:
        return 'Unknown'

def extract_table_text(table):

    pass

def scrap_status_webstore_react(the_store_set: set):
    driver = create_driver()
    result_the_store = {}
    number_of_orders = len(the_store_set)
    counter_for_done = 0

    try:
        for order_id in the_store_set:
            try:
                driver.get('https://www.webstaurantstore.com/trackorder.html')
                print('came to titel')
                # Wait for the email field to be present
                email_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'emailAddress-login-form'))
                )
                print('order-email loaded')
                order_number_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'orderNumber-login-form'))
                )
                print('order-number loaded')

                email_field.send_keys('kaatzeboo@gmail.com')
                order_number_field.send_keys(str(order_id))
                print('keys sended')
                # Wait for the submit button to be clickable
                submit_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, 'track-order'))
                )
                submit_button.click()
                print('clicked button')
                # Verify if the current page is the order status page
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#main > div:nth-child(4) > div > div:nth-child(1) > div > div > div.w-full.xl\:w-72.lg\:mr-6.xl\:mr-12 > div > ul:nth-child(1) > li:nth-child(3)'))
                )
                print(driver.current_url)
                # Extraction of status info 
                status_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#main > div:nth-child(4) > div > div:nth-child(1) > div > div > div.w-full.xl\:w-72.lg\:mr-6.xl\:mr-12 > div > ul:nth-child(1) > li:nth-child(3)'))
                )
                print('fields loaded')
                status_html = status_element.get_attribute('outerHTML')
                status_info = extract_status(status_html)
                
                # Classification of orders
                if status_info == 'Shipped' or status_info == 'Processing':
                    try:
                        table = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '#simple-tracking-details > div > div > table > tbody'))
                        )
                        rows = table.find_elements(By.TAG_NAME, 'tr')
                        table_data = []
                        for row in rows:
                            # Extract data from each cell in the row
                            date = row.find_element(By.CSS_SELECTOR, 'td:nth-child(1) span').text.strip()
                            carrier = row.find_element(By.CSS_SELECTOR, 'td:nth-child(2)').text.strip()
                            tracking_number = row.find_element(By.CSS_SELECTOR, 'td:nth-child(3)').text.strip()
                            tracking_link = row.find_element(By.CSS_SELECTOR, 'td:nth-child(4) a').get_attribute('href')

                            # Append the extracted data to the list
                            table_data.append({
                                'Date': date,
                                'Carrier': carrier,
                                'Tracking Number': tracking_number,
                                'Tracking Link': tracking_link
                            })
                    except TimeoutError:
                        # Handle case where table is not found
                        table_data = 'None'
                else:
                    table_data = 'None'

                    

                result_the_store[order_id] = [status_info, table_data]

            except Exception as e:
                print(f"An error occurred for order ID {order_id}: {e}")
                result_the_store[order_id] = ['Error']

            finally:
                time.sleep(5)
                counter_for_done += 1
                print(f'Order done: {counter_for_done}/{number_of_orders}')
                
    finally:
        driver.quit()
 
    return result_the_store


test_set = {'1010526533', '1010590295', '1010447527'}
print(scrap_status_webstore_react(test_set))