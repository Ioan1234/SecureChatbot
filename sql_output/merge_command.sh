#!/bin/bash

echo '-- Combined inserts generated on 2025-03-22 18:22:40.214893' > all_inserts_1000.sql
echo '' >> all_inserts_1000.sql
cat traders_inserts.sql >> all_inserts_1000.sql
cat brokers_inserts.sql >> all_inserts_1000.sql
cat assets_inserts.sql >> all_inserts_1000.sql
cat markets_inserts.sql >> all_inserts_1000.sql
cat trades_inserts.sql >> all_inserts_1000.sql
cat accounts_inserts.sql >> all_inserts_1000.sql
cat transactions_inserts.sql >> all_inserts_1000.sql
cat orders_inserts.sql >> all_inserts_1000.sql
cat order_status_inserts.sql >> all_inserts_1000.sql
cat price_history_inserts.sql >> all_inserts_1000.sql

echo 'All data merged to all_inserts_1000.sql'
