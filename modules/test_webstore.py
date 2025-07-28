from bs4 import BeautifulSoup
import requests

mail = 'kaatzeboo@gmail.com'
order_num = '99397842'

def create_tracking_url(order_number, email):
    base_url = "https://www.webstaurantstore.com/myaccount:trackorder/ordertracking/order_number/*/email/*/"
    tracking_url = base_url.replace("order_number/*", f"order_number/{order_number}").replace("email/*", f"email/{email}")
    return tracking_url

def track_order(order_number, email):
    result_web_store = {}

    url = create_tracking_url(order_number, email)
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Scrape the data from the specified element
        order_header = soup.select_one('#main > div > div > div > div.order__header > div > strong')
        
        if order_header:
            order_data = order_header.get_text(strip=True)
            # Split the text and take the part after the colon
            if ":" in order_data:
                order_data = order_data.split(":")[1].strip()
        else:
            print("Order header element not found.")

        tracker_web = ''
        if order_data == 'Processing':
            tracker_web = 'None'
        elif order_data == 'Shipped' or order_data == 'On The Way':
            table = soup.select_one('#orderTracking > div > div.order__tracking-content.clearfix > div.order__packages-container > table')
            if table:
                print("Table found.")
                tracking_number = None
                for cell in table.find_all('td'):
                    if 'Tracking No:' in cell.text:
                        tracking_number = cell.text.split('Tracking No:')[1].strip().split()[0]
                        break
                if tracking_number:
                    tracker_web = tracking_number
                else:
                    print("Tracking number not found in table.")
            else:
                print("Table element not found.")
        elif order_data == 'Delivered':
            tracker_web = ''

    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None

    result_web_store[order_number] = (order_data, tracker_web)
    return result_web_store

# Example usage
order_data = track_order(order_num, mail)

if order_data:
    print(order_data)
else:
    print("No data found.")
