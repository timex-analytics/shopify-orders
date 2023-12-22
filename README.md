# shopify-orders

## About this project
This is a Google Cloud Function aimed to make get requests to the Shopify Admin REST API Orders endpoint. The shopify-orders function is triggered daily by a Cloud Schedule Job ([daily_shopify_orders_sync](https://console.cloud.google.com/cloudscheduler?project=timexdtcga4)). All storefronts are requested getting the orders updated since the previous day (default, see last_update param). The resulting json file for each storefront is uploaded to the storefronts' respective directory in the GCS bucket, shopify-storefronts.
