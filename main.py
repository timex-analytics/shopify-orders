import functions_framework
import requests
import time
from datetime import datetime, timedelta
from google.cloud import storage
import os
import json
import re

#Variable controls how far back the API call looks for updates, running daily allows for lookback of 1 day
last_update = (datetime.now().date() + timedelta(days=-1)).isoformat()

@functions_framework.http
def get_shopify_orders(request):

    request_json = request.get_json(silent=True)
    request_args = request.args
    
    api_version = '2023-07'

    bucket_name = 'shopify-storefronts'

    #Timex Storefront APIs
    api_key_us = os.environ["shopify_api_key_us"]
    api_key_ca = os.environ["shopify_api_key_ca"]
    api_key_uk = os.environ["shopify_api_key_uk"]
    api_key_eu = os.environ["shopify_api_key_eu"]
    #Guess Storefront API
    api_key_guess = os.environ["shopify_api_key_guess"]
    #Daniel Wellington Storefront APIs
    api_key_dw_us = os.environ["shopify_api_key_dw_us"]
    api_key_dw_ca = os.environ["shopify_api_key_dw_ca"]
    api_key_dw_uk = os.environ["shopify_api_key_dw_uk"]

    #Timex Storefront Domains
    shop_domain_us = 'timexstorefront.myshopify.com'
    shop_domain_ca = 'timexstorefront-canada.myshopify.com'
    shop_domain_uk = 'timexstorefront-uk.myshopify.com'
    shop_domain_eu = 'timexstorefront-eu.myshopify.com'
    #Guess Storefront Domain
    shop_domain_guess = 'guesswatchesstorefront.myshopify.com'
    #Daniel Wellington Storefront Domains
    shop_domain_dw_us = 'daniel-wellington-us.myshopify.com'
    shop_domain_dw_ca = 'daniel-wellington-canada.myshopify.com'
    shop_domain_dw_uk = 'daniel-wellington-uk.myshopify.com'

    today = datetime.now().date()

    #Timex Filenames
    file_name_us = "timex.com/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    file_name_ca = "timex.ca/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    file_name_uk = "timex.co.uk/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    file_name_eu = "timex.eu/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    #Guess Filename
    file_name_guess = "guesswatches/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    #Daniel Wellington Filenames
    file_name_dw_us = "us.danielwellington.com/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    file_name_dw_ca = "ca.danielwellington.com/orders/orders_"+ today.strftime('%Y%m%d') +".json"
    file_name_dw_uk = "uk.danielwellington.com/orders/orders_"+ today.strftime('%Y%m%d') +".json"

    #Call get orders function for each storefront
    shopify_data_us = get_orders(api_key_us, api_version, shop_domain_us)
    print(f'Successfully retrieved {file_name_us}')
    upload_to_gcs(bucket_name, file_name_us, shopify_data_us)
    print(f'Successfully uploaded {file_name_us}')

    shopify_data_ca = get_orders(api_key_ca, api_version, shop_domain_ca)
    print(f'Successfully retrieved {file_name_ca}')
    upload_to_gcs(bucket_name, file_name_ca, shopify_data_ca)
    print(f'Successfully uploaded {file_name_ca}')

    shopify_data_uk = get_orders(api_key_uk, api_version, shop_domain_uk)
    print(f'Successfully retrieved {file_name_uk}')
    upload_to_gcs(bucket_name, file_name_uk, shopify_data_uk)
    print(f'Successfully uploaded {file_name_uk}')

    shopify_data_eu = get_orders(api_key_eu, api_version, shop_domain_eu)
    print(f'Successfully retrieved {file_name_eu}')
    upload_to_gcs(bucket_name, file_name_eu, shopify_data_eu)
    print(f'Successfully uploaded {file_name_eu}')

    shopify_data_guess = get_orders(api_key_guess, api_version, shop_domain_guess)
    print(f'Successfully retrieved {file_name_guess}')
    upload_to_gcs(bucket_name, file_name_guess, shopify_data_guess)
    print(f'Successfully uploaded {file_name_guess}')
    
    shopify_data_dw_us = get_orders(api_key_dw_us, api_version, shop_domain_dw_us)
    print(f'Successfully retrieved {file_name_dw_us}')
    upload_to_gcs(bucket_name, file_name_dw_us, shopify_data_dw_us)
    print(f'Successfully uploaded {file_name_dw_us}')

    shopify_data_dw_ca = get_orders(api_key_dw_ca, api_version, shop_domain_dw_ca)
    print(f'Successfully retrieved {file_name_dw_ca}')
    upload_to_gcs(bucket_name, file_name_dw_ca, shopify_data_dw_ca)
    print(f'Successfully uploaded {file_name_dw_ca}')

    shopify_data_dw_uk = get_orders(api_key_dw_uk, api_version, shop_domain_dw_uk)
    print(f'Successfully retrieved {file_name_dw_uk}')
    upload_to_gcs(bucket_name, file_name_dw_uk, shopify_data_dw_uk)
    print(f'Successfully uploaded {file_name_dw_uk}')

    print('Successfully wrote all Shopify data from Shopify Admin API to Cloud Storage.')
    return('Success')

#GET product data from Shopify store admin API
def get_orders(api_key, api_version, shop_domain):

    print(f"Attempting to get orders from Shopify, {shop_domain}.")
    #Set request url using respective shop domain for each storefront, current api version, and setting the request limit to the 250 max
    url = f"https://{shop_domain}/admin/api/{api_version}/orders.json?limit=250&updated_at_min={last_update}&status=any"
    print(f"URL: {url}")
    payload = {}
    headers = {
    'X-Shopify-Access-Token': api_key,
    'Content-Type': 'application/json',
    'Timeout' : '5'
    }
    #Create an json obj to push each page of orders to
    orders = {"orders" : []}
    #Variable for checking if there is another page of product data
    hasNextPage = 1
    print(f"Initiating request to shopify.")
    #While there is another page of product data, continue making requests
    while hasNextPage == 1:
        #Make request to appropriate url and set response to response var
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            #If no exceptions, carry out the code
            response.raise_for_status()
            #Append the order data from response to orders list var
            #https://shopify.dev/docs/api/admin-rest/2023-10/resources/order#resource-object
            for order in response.json()["orders"]:
                
                custom_line_items = {"line_items" : []}
                for item in order['line_items']:
                    custom_line_items['line_items'].append({
                        'id' : item['id'],
                        'admin_graphql_api_id' : item['admin_graphql_api_id'],
                        'gift_card' : item['gift_card'],
                        'name' : item['name'],
                        # 'pre_tax_price' : item['pre_tax_price'],
                        'price' : item['price'],
                        'product_exists' : item['product_exists'],
                        'product_id' : item['product_id'],
                        'quantity' : item['quantity'],
                        'sku' : item['sku'],
                        'taxable' : item['taxable'],
                        'title' : item['title'],
                        'total_discount' : item['total_discount'],
                        'variant_id' : item['variant_id'],
                        'variant_title' : item['variant_title'],
                        'vendor' : item['vendor']
                    })
                
                orders["orders"].append({
                    'id' : order['id'],
                    'admin_graphql_api_id' : order['admin_graphql_api_id'],
                    'cancel_reason' : order['cancel_reason'],
                    'cancelled_at' : order['cancelled_at'],
                    'confirmation_number' : order['confirmation_number'],
                    'created_at' : order['created_at'],
                    'currency' : order['currency'],
                    'current_subtotal_price' : order['current_subtotal_price'],
                    'current_total_discounts' : order['current_total_discounts'],
                    'current_total_price' : order['current_total_price'],
                    'current_total_tax' : order['current_total_tax'],
                    'customer_locale' : order['customer_locale'],
                    'discount_codes' : order['discount_codes'],
                    'financial_status' : order['financial_status'],
                    'fulfillment_status' : order['fulfillment_status'],
                    'name' : order['name'],
                    'note' : order['note'],
                    'note_attributes' : order['note_attributes'],
                    'number' : order['number'],
                    'order_number' : order['order_number'],
                    'order_status_url' : order['order_status_url'],
                    'processed_at' : order['processed_at'],
                    'subtotal_price' : order['subtotal_price'],
                    'tags' : order['tags'],
                    'taxes_included' : order['taxes_included'],
                    'test' : order['test'],
                    'token' : order['token'],
                    'total_discounts' : order['total_discounts'],
                    'total_line_items_price' : order['total_line_items_price'],
                    'total_outstanding' : order['total_outstanding'],
                    'total_price' : order['total_price'],
                    'total_shipping' : order['total_shipping_price_set']['shop_money']['amount'],
                    'total_tax' : order['total_tax'],
                    'updated_at' : order['updated_at'],
                    'user_id' : order['user_id'],
                    'line_items' : custom_line_items['line_items']
                })
            #Check if the response headers contains the link param and if the link param has a url with the relation flag set to "next"
            if ('link' in response.headers) and re.search("rel=\"next\"", response.headers['link']):
                #If both requirements are true, take the link param value and split the value on the semi-colon (this gives both the previous and next link in separate elements if both are available)
                link = response.headers["link"].split(",")
                #For each element from the split link param, check if the rel flag is set to "next"
                for element in link:
                    #If the rel flag is set to next, take the url from that element and set it as the new url for the next request.
                    if re.search("rel=\"next\"", element):
                        url = element.split("; ")[0].strip()[1:-1]
            #If the response headers do not contain a link param or the link param does not have a url with relation flag set to next (aka no next page of orders), set hasNextPage var to 0 to stop while loop            
            else:
                hasNextPage = 0
        except requests.exceptions.HTTPError as errh:
            raise(errh)
        except requests.exceptions.ConnectionError as errc:
            raise(errc)
        except requests.exceptions.Timeout as errt:
            raise(errt)
        except requests.exceptions.RequestException as err:
            raise(err)
        except Exception as e:
            raise(e)

    return orders["orders"]

#Write data to daily file
def upload_to_gcs(bucket_name, file_name, data):
    print(f"Attempting to write to cloud storage, {file_name}.")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = '\n'.join([json.dumps(item) for item in data])
        blob.upload_from_string(data=content, content_type="application/json")
    except Exception as e:
        raise(e)
    print(f"Successfully wrote {file_name} to cloud storage.")
