from bs4 import BeautifulSoup
import requests

def create_tracking_url(order_number):
    base_url = "https://www.webstaurantstore.com/myaccount:trackorder/ordertracking/order_number/*/email/*/"
    tracking_url = base_url.replace("order_number/*", f"order_number/{order_number}").replace("email/*", "email/kaatzeboo@gmail.com")
    return tracking_url

def track_order_web(order_numbers: set):
    result_web_store = {}
    for order_number in order_numbers:
        try:
            url = create_tracking_url(order_number)
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                order_header = soup.select_one('#main > div > div > div > div.order__header > div > strong')

                if order_header:
                    order_data = order_header.get_text(strip=True)
                    if ":" in order_data:
                        order_data = order_data.split(":")[1].strip()
                else:
                    print(f"Order header element not found for order {order_number}.")
                    order_data = 'Unknown'

                tracker_web = ''
                if order_data == 'Processing':
                    tracker_web = 'None'
                elif order_data == 'Shipped' or order_data == 'On The Way' or order_data == 'Partially Shipped':
                    table = soup.select_one('#orderTracking > div > div.order__tracking-content.clearfix > div.order__packages-container > table')
                    if table:
                        print(f"Table found for order {order_number}.")
                        tracking_number = None
                        for cell in table.find_all('td'):
                            if 'Tracking No:' in cell.text:
                                tracking_number = cell.text.split('Tracking No:')[1].strip().split()[0]
                                break
                        if tracking_number:
                            tracker_web = tracking_number
                        else:
                            print(f"Tracking number not found in table for order {order_number}.")
                    else:
                        print(f"Table element not found for order {order_number}.")
                elif order_data == 'Delivered':
                    tracker_web = ''
            else:
                print(f"Failed to retrieve the page for order {order_number}. Status code: {response.status_code}")
                order_data = 'Failed'
                tracker_web = 'Failed'
        except Exception as e:
            print(f"An error occurred while processing order {order_number}: {e}")
            order_data = 'Error'
            tracker_web = 'Error'

        result_web_store[order_number] = (order_data, tracker_web)
    return result_web_store


# test_set = {103891475,103870493}
# print(track_order_web(test_set))