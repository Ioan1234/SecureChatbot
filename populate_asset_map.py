import requests
import mysql.connector
import os
import logging
from dotenv import load_dotenv
import time
import json
import random
import re
load_dotenv()
from load_config import load_config

config = load_config()
API_KEY   = config["credentials"]["twelvedata_api_key"]
DB_CONFIG = config["database"]

BASE_API_URL = "https://api.twelvedata.com"
REFERENCE_ENDPOINTS = {
    '/stocks': 'Stock',
    '/forex_pairs': 'Forex',
    '/cryptocurrencies': 'Cryptocurrency',
    '/etfs': 'ETF',
}
ACTIVE_SYMBOLS_FILE = "active_symbols.txt"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler("asset_population.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_reference_data(endpoint):
    url = f"{BASE_API_URL}{endpoint}"
    params = {'apikey': API_KEY, 'show_plan': 'false'}
    logger.info(f"Fetching data from {url}...")
    all_data = []
    page = 1
    fetch_more = True
    outputsize = 5000
    params['outputsize'] = outputsize

    while fetch_more:
        request_url = url
        current_params = params.copy()
        try:
            if endpoint in ['/funds', '/bonds']:
                 current_params['page'] = page
                 request_url = f"{url}?page={page}"

            logger.debug(f"Requesting Page {page} with params: {current_params}")
            response = requests.get(url, params=current_params, timeout=45)
            response.raise_for_status()
            data = response.json()

            data_list = []
            status = data.get("status")

            if isinstance(data, list):
                data_list = data
                fetch_more = False
            elif isinstance(data, dict) and status == "ok":
                if "data" in data and isinstance(data["data"], list):
                    data_list = data["data"]
                    fetch_more = False
                elif "result" in data and isinstance(data["result"], dict) and "list" in data["result"]:
                    data_list = data["result"]["list"]
                    total_count = data["result"].get("count", 0)
                    if not data_list or len(all_data) + len(data_list) >= total_count or len(data_list) < outputsize:
                        fetch_more = False
                    else:
                        page += 1
                else:
                    logger.warning(f"Unexpected OK response structure for {endpoint}: {list(data.keys())}")
                    fetch_more = False
            elif isinstance(data, dict) and status == "error":
                 logger.error(f"API Error for {endpoint} (Page {page}): {data.get('code')} - {data.get('message')}")
                 fetch_more = False
                 return None
            else:
                logger.warning(f"Unrecognized response structure for {endpoint} (Page {page})")
                fetch_more = False

            if data_list:
                 all_data.extend(data_list)
                 logger.info(f"Fetched {len(data_list)} records (Page {page}, Total Fetched: {len(all_data)})")
            elif fetch_more:
                 logger.warning(f"Received empty data list on page {page} for {endpoint}, stopping pagination.")
                 fetch_more = False


            if page > 100:
                logger.warning("Reached page limit (100), stopping pagination.")
                fetch_more = False

            if fetch_more:
                 time.sleep(1.1)

        except requests.exceptions.Timeout:
            logger.error(f"API request timed out for {url} (Page {page})")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request Error for {url} (Page {page}): {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Failed to decode API JSON response for {url} (Page {page}). Response: {response.text[:200]}...")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during fetch for {url} (Page {page}): {e}", exc_info=True)
            return None

    logger.info(f"Finished fetching for {endpoint}. Total records processed: {len(all_data)}")
    return all_data


def get_existing_symbols_with_broker_id(conn):
    existing_map = {}
    cursor = None
    logger.info("Querying database for existing asset symbols and broker IDs...")
    try:
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT `api_symbol`, `broker_id` FROM `assets` WHERE `api_symbol` IS NOT NULL"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            existing_map[row['api_symbol']] = row['broker_id']
        logger.info(f"Found {len(existing_map)} existing API symbols with broker info in the database.")
        return existing_map
    except mysql.connector.Error as err:
        logger.error(f"Database Error getting existing symbol map: {err}")
        return None
    finally:
        if cursor: cursor.close()


def get_existing_broker_ids(conn):
    broker_ids = []
    cursor = None
    logger.info("Querying database for existing broker IDs...")
    try:
        cursor = conn.cursor()
        sql = "SELECT `broker_id` FROM `brokers`"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            broker_ids.append(row[0])
        logger.info(f"Found {len(broker_ids)} existing broker IDs in the database.")
        return broker_ids if broker_ids else None
    except mysql.connector.Error as err:
        logger.error(f"Database Error getting existing broker IDs: {err}")
        return None
    finally:
        if cursor: cursor.close()


def process_and_sync_assets(conn, existing_symbol_map, available_broker_ids, api_data, asset_category_default):
    if api_data is None:
        logger.warning(f"No API data provided for category {asset_category_default}. Skipping sync.")
        return [], [], 0

    assets_to_insert_list = []
    symbols_needing_update_list = []
    processed_count = 0
    error_count = 0

    can_assign_broker = bool(available_broker_ids)
    if not can_assign_broker:
        logger.warning("No broker IDs found in the database. Cannot assign random broker_id.")

    logger.info(f"Processing {len(api_data)} records for category {asset_category_default}...")
    for item in api_data:
        processed_count += 1
        try:
            symbol = item.get('symbol')
            name = None

            if asset_category_default in ['Stock', 'ETF', 'Bond', 'Commodity', 'Fund']: name = item.get('name')
            elif asset_category_default in ['Forex', 'Cryptocurrency']:
                base = item.get('currency_base'); quote = item.get('currency_quote')
                if base and quote: name = f"{base}/{quote}"
            if not name: name = symbol

            api_type = item.get('type'); asset_type = api_type if api_type else asset_category_default

            if not symbol or not name:
                logger.warning(f"Skipping item due to missing symbol or derived name: {item}")
                error_count += 1; continue

            symbol = symbol.strip(); name = (name.strip()[:99]) if name else f"Unknown {symbol}"; asset_type = (asset_type.strip()[:49]) if asset_type else "Unknown"
            if not symbol:
                 logger.warning(f"Skipping item due to empty symbol after strip: {item}")
                 error_count += 1; continue

            current_broker_id = existing_symbol_map.get(symbol, '##NOT_FOUND##')

            if current_broker_id != '##NOT_FOUND##':
                if current_broker_id is None and can_assign_broker:
                    symbols_needing_update_list.append(symbol)
                continue
            else:
                random_broker_id = random.choice(available_broker_ids) if can_assign_broker else None
                assets_to_insert_list.append((name, asset_type, symbol, random_broker_id))
                existing_symbol_map[symbol] = random_broker_id

        except Exception as e:
            logger.error(f"Error processing item {item}: {e}", exc_info=False)
            error_count += 1

    logger.info(f"Finished processing {processed_count} records for {asset_category_default}. Prepared {len(assets_to_insert_list)} for insert, identified {len(symbols_needing_update_list)} for broker update.")
    return assets_to_insert_list, symbols_needing_update_list, error_count


def execute_asset_inserts(conn, assets_to_insert):
    if not assets_to_insert:
        logger.info("No new assets to insert.")
        return 0, 0

    inserted_count = 0
    error_count = 0
    logger.info(f"Attempting to insert {len(assets_to_insert)} new assets...")
    sql_insert = """
        INSERT INTO `assets` (`name`, `asset_type`, `api_symbol`, `broker_id`)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `asset_type`=VALUES(`asset_type`)
        """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.executemany(sql_insert, assets_to_insert)
        conn.commit()
        inserted_count = len(assets_to_insert)
        logger.info(f"DB insert/update operation completed for {inserted_count} assets (affected rows: {cursor.rowcount}).")
    except mysql.connector.Error as err:
        logger.error(f"Database Error inserting assets: {err}")
        conn.rollback()
        error_count = len(assets_to_insert)
        inserted_count = 0
    except Exception as e:
        logger.error(f"Unexpected error during DB insert: {e}", exc_info=True)
        conn.rollback()
        error_count = len(assets_to_insert)
        inserted_count = 0
    finally:
        if cursor: cursor.close()
    return inserted_count, error_count


def update_null_broker_ids(conn, symbols_to_update, available_broker_ids):
    if not symbols_to_update or not available_broker_ids:
        if not symbols_to_update: logger.info("No existing assets required broker ID updates.")
        if not available_broker_ids: logger.warning("Cannot update broker IDs because available_broker_ids list is empty.")
        return 0

    logger.info(f"Attempting to update broker_id for {len(symbols_to_update)} existing assets with NULL broker_id...")
    sql_update = "UPDATE `assets` SET `broker_id` = %s WHERE `api_symbol` = %s AND `broker_id` IS NULL"
    update_data = [(random.choice(available_broker_ids), symbol) for symbol in symbols_to_update]
    updated_count = 0
    cursor = None
    try:
        cursor = conn.cursor()
        for broker_id, symbol in update_data:
            cursor.execute(sql_update, (broker_id, symbol))
            updated_count += cursor.rowcount
        conn.commit()
        logger.info(f"Successfully updated broker_id for {updated_count} assets.")
        return updated_count
    except mysql.connector.Error as err:
        logger.error(f"Database Error updating broker IDs: {err}")
        conn.rollback(); return 0
    except Exception as e:
        logger.error(f"Unexpected error during broker ID update: {e}", exc_info=True)
        conn.rollback(); return 0
    finally:
        if cursor: cursor.close()


def load_active_symbols(filename=ACTIVE_SYMBOLS_FILE):
    active = set()
    logger.info(f"Loading active symbols from '{filename}'...")
    try:
        if not os.path.exists(filename):
            logger.error(f"CRITICAL: Active symbols file '{filename}' not found. Cannot determine which symbols to keep active. Aborting.")
            return None

        with open(filename, 'r') as f:
            for i, line in enumerate(f):
                symbol = line.strip()
                if symbol and not symbol.startswith('#'):
                    active.add(symbol)
                elif not symbol or symbol.startswith('#'):
                     logger.debug(f"Skipping line {i+1} in {filename} (empty or comment)")


        if not active:
            logger.error(f"CRITICAL: Active symbols file '{filename}' was found but contained no valid symbols. Aborting to prevent deactivating all assets.")
            return None

        logger.info(f"Loaded {len(active)} symbols to keep active from '{filename}'.")
        return active
    except Exception as e:
        logger.error(f"Error reading active symbols file '{filename}': {e}")
        return None


def delete_unwanted_assets_and_history(conn, active_symbols_set):
    if active_symbols_set is None:
        logger.error("Cannot delete assets: Active symbols list could not be loaded or was empty.")
        return 0

    deleted_asset_count = 0
    cursor = None

    try:
        cursor = conn.cursor(dictionary=True)

        assets_to_delete = []
        if not active_symbols_set:
             logger.error("Internal Error: delete_unwanted_assets called with empty active_symbols_set. Should have been caught earlier.")
             return 0

        placeholders = ', '.join(['%s'] * len(active_symbols_set))
        sql_find_inactive = f"""
             SELECT `asset_id`, `api_symbol` FROM `assets`
             WHERE `api_symbol` IS NOT NULL AND `api_symbol` NOT IN ({placeholders})
         """
        params_find = list(active_symbols_set)
        cursor.execute(sql_find_inactive, params_find)
        assets_to_delete = cursor.fetchall()


        if not assets_to_delete:
            logger.info("No inactive assets with api_symbol found for deletion.")
            return 0

        asset_ids_to_delete = [a['asset_id'] for a in assets_to_delete]
        asset_symbols_deleted = [a['api_symbol'] for a in assets_to_delete]
        log_symbols = asset_symbols_deleted[:10]
        if len(asset_symbols_deleted) > 10: log_symbols.append("...")
        logger.warning(f"Identified {len(asset_ids_to_delete)} assets for PERMANENT DELETION (and related data): {log_symbols}")

        conn.start_transaction()
        logger.info("Starting transaction for deletion...")

        id_placeholders = ', '.join(['%s'] * len(asset_ids_to_delete))
        params_delete = tuple(asset_ids_to_delete)

        tables_to_clear = {
            "price_history": "asset_id",
            "order_status": "order_id IN (SELECT order_id FROM orders WHERE trade_id IN (SELECT trade_id FROM trades WHERE asset_id IN ({0})))".format(id_placeholders),
            "orders": "trade_id IN (SELECT trade_id FROM trades WHERE asset_id IN ({0}))".format(id_placeholders),
            "trades": "asset_id"
        }

        if asset_ids_to_delete:
             sql_find_trades = f"SELECT trade_id FROM trades WHERE asset_id IN ({id_placeholders})"
             cursor.execute(sql_find_trades, params_delete)
             trade_results = cursor.fetchall()
             trade_ids_to_delete = tuple(t['trade_id'] for t in trade_results) if trade_results else None

             if trade_ids_to_delete:
                  trade_id_placeholders = ', '.join(['%s'] * len(trade_ids_to_delete))
                  sql_delete_order_status = f"DELETE FROM order_status WHERE order_id IN (SELECT order_id FROM orders WHERE trade_id IN ({trade_id_placeholders}))"
                  cursor.execute(sql_delete_order_status, trade_ids_to_delete)
                  logger.info(f"Deleted {cursor.rowcount} rows from order_status.")
                  sql_delete_orders = f"DELETE FROM orders WHERE trade_id IN ({trade_id_placeholders})"
                  cursor.execute(sql_delete_orders, trade_ids_to_delete)
                  logger.info(f"Deleted {cursor.rowcount} rows from orders.")
             else:
                  logger.info("No related orders/status found for assets being deleted.")

             sql_delete_trades = f"DELETE FROM trades WHERE asset_id IN ({id_placeholders})"
             cursor.execute(sql_delete_trades, params_delete)
             logger.info(f"Deleted {cursor.rowcount} rows from trades.")

             sql_delete_prices = f"DELETE FROM price_history WHERE asset_id IN ({id_placeholders})"
             cursor.execute(sql_delete_prices, params_delete)
             logger.info(f"Deleted {cursor.rowcount} rows from price_history.")

        sql_delete_assets = f"DELETE FROM assets WHERE asset_id IN ({id_placeholders})"
        cursor.execute(sql_delete_assets, params_delete)
        deleted_asset_count = cursor.rowcount
        logger.info(f"Deleted {deleted_asset_count} rows from assets.")

        conn.commit()
        logger.info("Deletion transaction committed successfully.")
        return deleted_asset_count

    except mysql.connector.Error as err:
        logger.error(f"Database Error during asset deletion: {err}")
        if conn: conn.rollback()
        logger.warning("Deletion transaction rolled back due to error.")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error during asset deletion: {e}", exc_info=True)
        if conn: conn.rollback()
        logger.warning("Deletion transaction rolled back due to error.")
        return 0
    finally:
        if cursor: cursor.close()

if __name__ == "__main__":
    logger.info("--- Starting Asset Population Script ---")

    if not API_KEY or API_KEY == "YOUR_REAL_TWELVEDATA_API_KEY":
        logger.error("API Key not configured in .env file. Please set TWELVEDATA_API_KEY.")
        exit(1)

    conn = None
    total_inserted = 0
    total_updated_broker = 0
    total_deleted = 0
    total_errors = 0

    try:
        logger.info(f"Connecting to database '{DB_CONFIG['database']}' on host '{DB_CONFIG['host']}'...")
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info("Database connection successful.")

        active_symbols = load_active_symbols()
        if active_symbols is None:
             logger.critical(f"Mandatory active symbols list failed to load. Aborting run.")
             if conn and conn.is_connected(): conn.close()
             exit(1)

        existing_symbol_map = get_existing_symbols_with_broker_id(conn)
        if existing_symbol_map is None:
             logger.error("Failed to fetch existing symbol map. Aborting.")
             if conn and conn.is_connected(): conn.close()
             exit(1)

        existing_broker_ids = get_existing_broker_ids(conn)
        if existing_broker_ids is None:
            logger.warning("Could not fetch broker IDs. Will not be able to assign/update random brokers.")


        all_assets_to_insert = []
        all_symbols_needing_update = []
        for endpoint, default_type in REFERENCE_ENDPOINTS.items():
            logger.info(f"\n--- Processing Endpoint: {endpoint} (Default Type: {default_type}) ---")
            api_asset_data = fetch_reference_data(endpoint)

            if api_asset_data is not None:
                insert_list, needs_update_list, errors = process_and_sync_assets(
                    conn,
                    existing_symbol_map,
                    existing_broker_ids,
                    api_asset_data,
                    default_type
                )
                all_assets_to_insert.extend(insert_list)
                all_symbols_needing_update.extend(needs_update_list)
                total_errors += errors
            else:
                logger.error(f"Failed to fetch or process data for {endpoint}.")
                total_errors += 1

        inserted_now, insert_errors = execute_asset_inserts(conn, all_assets_to_insert)
        total_inserted += inserted_now
        total_errors += insert_errors

        unique_symbols_to_update = list(set(all_symbols_needing_update))
        if unique_symbols_to_update and existing_broker_ids:
             updated = update_null_broker_ids(conn, unique_symbols_to_update, existing_broker_ids)
             total_updated_broker += updated
        elif unique_symbols_to_update:
            logger.warning(f"Found {len(unique_symbols_to_update)} assets needing broker ID update, but no broker IDs are available.")
        else:
             logger.info("No existing assets required broker ID updates across all endpoints.")

        deleted = delete_unwanted_assets_and_history(conn, active_symbols)
        total_deleted = deleted


    except mysql.connector.Error as err:
        logger.error(f"Database connection failed or main DB error: {err}")
        total_errors += 1
        if conn:
            try:
                conn.rollback()
                logger.info("Rolled back transaction due to main error.")
            except Exception as rb_err:
                logger.error(f"Error during rollback: {rb_err}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in main script: {e}", exc_info=True)
        total_errors += 1
        if conn:
            try:
                conn.rollback()
                logger.info("Rolled back transaction due to main error.")
            except Exception as rb_err:
                logger.error(f"Error during rollback: {rb_err}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.info("Database connection closed.")

    logger.info("\n--- Asset Population Summary ---")
    logger.info(f"Total New Assets Inserted/Processed: {total_inserted}")
    logger.info(f"Total Existing Assets Updated with Broker ID: {total_updated_broker}")
    logger.info(f"Total Assets DELETED (incl. related data): {total_deleted}")
    logger.info(f"Total Errors Encountered: {total_errors}")
    logger.info("--- Script Finished ---")