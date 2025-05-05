#!/bin/bash

# Merge all SQL files into all_inserts_100.sql
echo '-- Combined inserts generated on 2025-05-24 17:52:12.660997' > all_inserts_100.sql
echo '' >> all_inserts_100.sql
cat traders_inserts.sql >> all_inserts_100.sql
cat markets_inserts.sql >> all_inserts_100.sql
cat trades_inserts.sql >> all_inserts_100.sql
cat accounts_inserts.sql >> all_inserts_100.sql
cat transactions_inserts.sql >> all_inserts_100.sql
cat orders_inserts.sql >> all_inserts_100.sql
cat order_status_inserts.sql >> all_inserts_100.sql
cat price_history_inserts.sql >> all_inserts_100.sql

echo 'All data merged to all_inserts_100.sql'
