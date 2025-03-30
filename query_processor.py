import logging
import re
import datetime
from collections import defaultdict


class QueryProcessor:
    def __init__(self, db_connector, encryption_manager, sensitive_fields=None):
        self.logger = logging.getLogger(__name__)
        self.db_connector = db_connector
        self.encryption_manager = encryption_manager
        self.sensitive_fields = sensitive_fields or [
            "email", "contact_email", "phone", "license_number", "balance"
        ]

        self.schema = {
            "traders": ["trader_id", "name", "email", "phone", "registration_date"],
            "brokers": ["broker_id", "name", "license_number", "contact_email"],
            "assets": ["asset_id", "name", "asset_type", "broker_id"],
            "markets": ["market_id", "name", "location", "opening_time", "closing_time"],
            "trades": ["trade_id", "trader_id", "asset_id", "market_id", "trade_date", "quantity", "price"],
            "accounts": ["account_id", "trader_id", "balance", "account_type", "creation_date"],
            "transactions": ["transaction_id", "account_id", "transaction_date", "transaction_type", "amount"],
            "orders": ["order_id", "trade_id", "order_type", "order_date"],
            "order_status": ["status_id", "order_id", "status", "status_date"],
            "price_history": ["price_id", "asset_id", "price_date", "open_price", "close_price"]
        }

        self.relationships = {
            ("traders", "trades"): ("trader_id", "trader_id"),
            ("traders", "accounts"): ("trader_id", "trader_id"),
            ("assets", "trades"): ("asset_id", "asset_id"),
            ("assets", "price_history"): ("asset_id", "asset_id"),
            ("markets", "trades"): ("market_id", "market_id"),
            ("brokers", "assets"): ("broker_id", "broker_id"),
            ("accounts", "transactions"): ("account_id", "account_id"),
            ("trades", "orders"): ("trade_id", "trade_id"),
            ("orders", "order_status"): ("order_id", "order_id")
        }

        self.query_patterns = self._init_query_patterns()

        self.analytical_patterns = self._init_analytical_patterns()

    def _init_query_patterns(self):
        patterns = {
            "count": [
                r'how many (\w+)',
                r'count (?:of|the) (\w+)',
                r'total (?:number of)? (\w+)',
                r'number of (\w+)'
            ],

            "existence": [
                r'are there (?:any)? (\w+)',
                r'do (?:any)? (\w+) exist',
                r'(?:any|some) (\w+) (?:that|who|which)'
            ],

            "average": [
                r'average (\w+)',
                r'mean (\w+)',
                r'typical (\w+)'
            ],

            "maximum": [
                r'highest (\w+)',
                r'maximum (\w+)',
                r'largest (\w+)',
                r'biggest (\w+)',
                r'most (\w+)'
            ],

            "minimum": [
                r'lowest (\w+)',
                r'minimum (\w+)',
                r'smallest (\w+)',
                r'least (\w+)'
            ],

            "time": [
                r'before (\d{4}-\d{2}-\d{2})',
                r'after (\d{4}-\d{2}-\d{2})',
                r'between (\d{4}-\d{2}-\d{2}) and (\d{4}-\d{2}-\d{2})',
                r'on (\d{4}-\d{2}-\d{2})',
                r'earliest',
                r'latest',
                r'oldest',
                r'newest'
            ],

            "distribution": [
                r'distribution of (\w+)',
                r'breakdown of (\w+)',
                r'grouped by (\w+)'
            ],

            "nulls": [
                r'without (?:a|an)? (\w+)',
                r'missing (\w+)',
                r'null (\w+)',
                r'empty (\w+)'
            ],

            "traders": [
                r'traders?',
                r'users?',
                r'customers?',
                r'clients?',
                r'people'
            ],

            "markets": [
                r'markets?',
                r'exchanges?',
                r'trading platforms?'
            ],

            "assets": [
                r'assets?',
                r'stocks?',
                r'etfs?',
                r'bonds?',
                r'securities?',
                r'cryptocurrenc(?:y|ies)',
                r'commodit(?:y|ies)'
            ],

            "trade_price_range": {
                "patterns": [
                    r'price range for trades',
                    r'trade price range',
                    r'price range of trades'
                ],
                "sql_template": """
              SELECT
                MIN(price)   AS min_price,
                MAX(price)   AS max_price,
                AVG(price)   AS avg_price,
                STDDEV(price) AS price_stddev
              FROM trades
            """
            }
        }

        compiled_patterns = {}
        for category, pattern_list in patterns.items():
            compiled_patterns[category] = [re.compile(pattern, re.IGNORECASE) for pattern in pattern_list]

        return compiled_patterns

    def _init_analytical_patterns(self):
        patterns = {
            "traders_count_before_date": {
                "patterns": [
                    r'how many traders (?:have|were) registered before ([\d-]+)',
                    r'count (?:of|the) traders before ([\d-]+)'
                ],
                "sql_template": """
                    SELECT COUNT(*) as count 
                    FROM traders 
                    WHERE registration_date < '{date}'
                """
            },

            "traders_total_count": {
                "patterns": [
                    r'(?:what is|how many|count) (?:the)? total number of traders',
                    r'how many traders are (?:there|in the system)',
                    r'count all traders'
                ],
                "sql_template": """
                    SELECT COUNT(*) as count 
                    FROM traders
                """
            },

            "traders_without_contact": {
                "patterns": [
                    r'(?:are there|any|find|list) traders without (?:an|a)? (email|phone)',
                    r'traders (?:with|having) (?:no|missing) (email|phone)'
                ],
                "sql_template": """
                    SELECT COUNT(*) as count, 
                           (SELECT COUNT(*) FROM traders WHERE {field} IS NULL OR {field} = '') as without_field 
                    FROM traders
                """
            },

            "common_email_domain": {
                "patterns": [
                    r'(?:what is|find) the most common (?:email)? domain',
                    r'popular email domains'
                ],
                "sql_template": """
                    SELECT 
                        SUBSTRING_INDEX(email, '@', -1) as domain,
                        COUNT(*) as count
                    FROM traders
                    WHERE email IS NOT NULL AND email != ''
                    GROUP BY domain
                    ORDER BY count DESC
                    LIMIT 10
                """
            },

            "traders_same_name": {
                "patterns": [
                    r'how many traders share the same name',
                    r'traders with identical names'
                ],
                "sql_template": """
                    SELECT name, COUNT(*) as count
                    FROM traders
                    GROUP BY name
                    HAVING count > 1
                    ORDER BY count DESC
                """
            },

            "oldest_traders": {
                "patterns": [
                    r'(?:who are|which|find) the (?:traders with|oldest) (?:the oldest|earliest) registration dates',
                ],
                "sql_template": """
                    SELECT *
                    FROM traders
                    ORDER BY registration_date ASC
                    LIMIT 10
                """
            },

            "traders_without_trades": {
                "patterns": [
                    r'(?:are|any|which) traders (?:who|that) haven\'t made any trades',
                    r'traders without trades'
                ],
                "sql_template": """
                    SELECT t.*
                    FROM traders t
                    LEFT JOIN trades tr ON t.trader_id = tr.trader_id
                    WHERE tr.trade_id IS NULL
                """
            },

            "traders_multiple_accounts": {
                "patterns": [
                    r'how many traders have multiple accounts',
                    r'traders with more than one account'
                ],
                "sql_template": """
                    SELECT t.trader_id, t.name, COUNT(a.account_id) as account_count
                    FROM traders t
                    JOIN accounts a ON t.trader_id = a.trader_id
                    GROUP BY t.trader_id, t.name
                    HAVING account_count > 1
                    ORDER BY account_count DESC
                """
            },

            "average_registration_date": {
                "patterns": [
                    r'what is the average registration date',
                    r'mean date of registration'
                ],
                "sql_template": """
                    SELECT 
                        AVG(UNIX_TIMESTAMP(registration_date)) as avg_timestamp,
                        FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(registration_date))) as avg_date
                    FROM traders
                """
            },

            "trader_registration_distribution": {
                "patterns": [
                    r'(?:what is|show) the distribution of traders by year',
                    r'traders grouped by registration year'
                ],
                "sql_template": """
                    SELECT 
                        YEAR(registration_date) as year,
                        COUNT(*) as count
                    FROM traders
                    GROUP BY year
                    ORDER BY year
                """
            },

            "markets_count": {
                "patterns": [
                    r'how many markets (?:exist|are there)',
                    r'count (?:of|the) markets'
                ],
                "sql_template": """
                    SELECT COUNT(*) as count
                    FROM markets
                """
            },

            "market_trading_hours": {
                "patterns": [
                    r'what are the (?:opening|closing) (?:times|hours) of (?:each|the) market',
                    r'market (?:hours|times)'
                ],
                "sql_template": """
                    SELECT 
                        name, 
                        opening_time, 
                        closing_time,
                        TIMEDIFF(closing_time, opening_time) as trading_duration
                    FROM markets
                    ORDER BY opening_time
                """
            },

            "earliest_latest_markets": {
                "patterns": [
                    r'which market opens (?:the)? earliest',
                    r'which (?:market|one) closes (?:the)? latest'
                ],
                "sql_template": """
                    SELECT 
                        (SELECT name FROM markets ORDER BY opening_time ASC LIMIT 1) as earliest_opening,
                        (SELECT opening_time FROM markets ORDER BY opening_time ASC LIMIT 1) as earliest_time,
                        (SELECT name FROM markets ORDER BY closing_time DESC LIMIT 1) as latest_closing,
                        (SELECT closing_time FROM markets ORDER BY closing_time DESC LIMIT 1) as latest_time
                """
            },

            "overlapping_markets": {
                "patterns": [
                    r'(?:are|any|which) markets (?:with|having) overlapping (?:trading)? hours',
                ],
                "sql_template": """
                    SELECT 
                        m1.name as market1, 
                        m2.name as market2,
                        m1.opening_time as m1_open,
                        m1.closing_time as m1_close,
                        m2.opening_time as m2_open,
                        m2.closing_time as m2_close
                    FROM markets m1
                    JOIN markets m2 ON m1.market_id < m2.market_id
                    WHERE 
                        (m1.opening_time BETWEEN m2.opening_time AND m2.closing_time) OR
                        (m1.closing_time BETWEEN m2.opening_time AND m2.closing_time) OR
                        (m2.opening_time BETWEEN m1.opening_time AND m1.closing_time) OR
                        (m2.closing_time BETWEEN m1.opening_time AND m1.closing_time)
                """
            },

            "markets_by_location": {
                "patterns": [
                    r'how many markets (?:are|located) in (?:a specific )?(city|region|location)?'
                ],
                "sql_template": """
                    SELECT 
                        location,
                        COUNT(*) as count
                    FROM markets
                    GROUP BY location
                    ORDER BY count DESC
                """
            },

            "market_name_uniqueness": {
                "patterns": [
                    r'do all markets have unique names',
                    r'duplicate market names'
                ],
                "sql_template": """
                    SELECT 
                        name,
                        COUNT(*) as name_count
                    FROM markets
                    GROUP BY name
                    HAVING name_count > 1
                """
            },

            "busiest_market": {
                "patterns": [
                    r'what is the busiest market',
                    r'market with (?:the)? most trades'
                ],
                "sql_template": """
                    SELECT 
                        m.market_id,
                        m.name,
                        COUNT(t.trade_id) as trade_count
                    FROM markets m
                    LEFT JOIN trades t ON m.market_id = t.market_id
                    GROUP BY m.market_id, m.name
                    ORDER BY trade_count DESC
                    LIMIT 10
                """
            },

            "highest_account_balance": {
                "patterns": [
                    r'which trader has (?:the)? highest (?:account)? balance',
                    r'trader with most money',
                    r'largest account balance'
                ],
                "sql_template": """
                    SELECT 
                        t.trader_id,
                        t.name,
                        a.account_id,
                        a.balance,
                        a.account_type
                    FROM traders t
                    JOIN accounts a ON t.trader_id = a.trader_id
                    ORDER BY a.balance DESC
                    LIMIT 10
                """
            },

            "account_balance_stats": {
                "patterns": [
                    r'what is the average account balance',
                    r'mean balance (?:across|of) accounts'
                ],
                "sql_template": """
                    SELECT 
                        COUNT(*) as total_accounts,
                        AVG(balance) as avg_balance,
                        MIN(balance) as min_balance,
                        MAX(balance) as max_balance,
                        SUM(balance) as total_balance
                    FROM accounts
                """
            },

            "negative_balance_accounts": {
                "patterns": [
                    r'how many accounts have (?:a)? negative (?:or zero)? balance',
                    r'accounts with (?:balance|balances) (?:below|less than) zero'
                ],
                "sql_template": """
                    SELECT 
                        COUNT(*) as total_accounts,
                        SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END) as negative_balance_count,
                        SUM(CASE WHEN balance = 0 THEN 1 ELSE 0 END) as zero_balance_count,
                        SUM(CASE WHEN balance <= 0 THEN 1 ELSE 0 END) as non_positive_balance_count
                    FROM accounts
                """
            },

            "account_types": {
                "patterns": [
                    r'what (?:are|is) the different account types',
                    r'types of accounts (?:available)?'
                ],
                "sql_template": """
                    SELECT 
                        account_type,
                        COUNT(*) as count
                    FROM accounts
                    GROUP BY account_type
                    ORDER BY count DESC
                """
            },

            "trader_multiple_accounts": {
                "patterns": [
                    r'(?:are|any|which) traders (?:with|having) multiple accounts',
                    r'traders (?:that|who) have more than one account'
                ],
                "sql_template": """
                    SELECT 
                        t.trader_id,
                        t.name,
                        COUNT(a.account_id) as account_count
                    FROM traders t
                    JOIN accounts a ON t.trader_id = a.trader_id
                    GROUP BY t.trader_id, t.name
                    HAVING account_count > 1
                    ORDER BY account_count DESC
                """
            },

            "most_common_transaction_type": {
                "patterns": [
                    r'what is the most common transaction type',
                    r'popular transaction (?:types|categories)'
                ],
                "sql_template": """
                    SELECT 
                        transaction_type,
                        COUNT(*) as count
                    FROM transactions
                    GROUP BY transaction_type
                    ORDER BY count DESC
                """
            },

            "transaction_amount_range": {
                "patterns": [
                    r'what is the (?:highest|lowest) transaction amount',
                    r'range of transaction (?:amounts|values)'
                ],
                "sql_template": """
                    SELECT 
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount,
                        AVG(amount) as avg_amount,
                        SUM(amount) as total_amount
                    FROM transactions
                """
            }

        }

        for key, pattern_data in patterns.items():
            pattern_data["compiled_patterns"] = [
                re.compile(pattern, re.IGNORECASE) for pattern in pattern_data["patterns"]
            ]

        return patterns

    def process_query(self, nl_query, intent_data=None):
        self.logger.info(f"Processing query: {nl_query}")

        analytical = self._match_analytical_pattern(nl_query)
        if analytical:
            return self._execute_analytical_query(analytical, nl_query)

        intent = intent_data.get("intent")
        sub_intent = intent_data.get("sub_intent")
        entities = self._extract_entities(nl_query)
        tables = entities.get("tables", [])

        if intent_data and intent_data.get("intent") == "database_query_comparative":
            result = self._execute_generic_comparative(nl_query)
            if result is not None:
                return result


        query_type = self._determine_query_type(nl_query)
        entities    = self._extract_entities(nl_query)
        sql         = self._generate_sql(query_type, entities, nl_query)
        if not sql:
            return None
        return self._execute_and_process_query(sql)

    def _execute_generic_comparative(self, nl_query: str):
        main_table = None
        for tbl in self.schema:
            if re.search(rf"\b{tbl}\b", nl_query, re.IGNORECASE):
                main_table = tbl
                break
        if not main_table:
            return None

        related = None
        pk_main = pk_rel = None
        for (t1, t2), (k1, k2) in self.relationships.items():
            if t1 == main_table:
                related, pk_main, pk_rel = t2, k1, k2
                break
            if t2 == main_table:
                related, pk_main, pk_rel = t1, k2, k1
                break
        if not related:
            return None

        sql = f"""
             SELECT
               m.{pk_main}     AS id,
               m.name          AS name,
               COUNT(r.{pk_rel}) AS count
             FROM {main_table} m
             JOIN {related} r
               ON m.{pk_main} = r.{pk_rel}
             GROUP BY m.{pk_main}, m.name
             ORDER BY count DESC
             LIMIT 1
           """

        result = self.db_connector.execute_encrypted_query(
            "SELECT", [main_table, related], fields=None, conditions=None,
            order_by=None, limit=1
        )
        if not result:
            return None

        return {
            "response": f"Top {main_table.rstrip('s').capitalize()}: {result[0]['name']} ({result[0]['count']} records)",
            "data": result
        }

    def _match_analytical_pattern(self, nl_query):
        for name, pattern_data in self.analytical_patterns.items():
            for pattern in pattern_data["compiled_patterns"]:
                match = pattern.search(nl_query)
                if match:
                    params = match.groups()

                    return {
                        "name": name,
                        "sql_template": pattern_data["sql_template"],
                        "params": params
                    }
        return None

    def _execute_analytical_query(self, analytical_query, nl_query):
        sql_template = analytical_query["sql_template"]
        params = analytical_query["params"]

        sql = sql_template

        if analytical_query["name"] == "traders_count_before_date" and params:
            sql = sql.format(date=params[0])
        elif analytical_query["name"] == "traders_without_contact" and params:
            field = params[0].lower()
            sql = sql.format(field=field)
        elif "date" in sql and params and len(params) > 0:
            sql = sql.format(date=params[0])
        elif "field" in sql and params and len(params) > 0:
            sql = sql.format(field=params[0])

        result = self._execute_and_process_query(sql)
        return result

    def _determine_query_type(self, nl_query):
        query_type = defaultdict(int)

        query_lower = nl_query.lower()

        for category, patterns in self.query_patterns.items():
            for pattern in patterns:
                matches = pattern.findall(query_lower)
                if matches:
                    query_type[category] += len(matches)

        if not query_type:
            return "list"

        primary_type = max(query_type.items(), key=lambda x: x[1])[0]

        if primary_type == "count":
            return "count"
        elif primary_type == "average":
            return "aggregate_avg"
        elif primary_type == "maximum":
            return "aggregate_max"
        elif primary_type == "minimum":
            return "aggregate_min"
        elif primary_type == "nulls":
            return "nulls"
        elif primary_type == "time":
            return "time"
        elif primary_type == "distribution":
            return "distribution"
        elif primary_type == "existence":
            return "existence"
        else:
            return "list"

    def _extract_entities(self, nl_query):
        entities = {
            "tables": [],
            "fields": [],
            "conditions": [],
            "order": None,
            "limit": 100
        }

        query_lower = nl_query.lower()

        for table in self.schema.keys():
            singular = table[:-1] if table.endswith('s') else table
            patterns = [
                re.compile(fr'\b{table}\b', re.IGNORECASE),
                re.compile(fr'\b{singular}\b', re.IGNORECASE)
            ]

            for pattern in patterns:
                if pattern.search(query_lower):
                    entities["tables"].append(table)
                    break

        if not entities["tables"]:
            for entity_type, patterns in self.query_patterns.items():
                if entity_type in ["traders", "markets", "assets"]:
                    for pattern in patterns:
                        if pattern.search(query_lower):
                            entities["tables"].append(entity_type)
                            break

        if not entities["tables"]:
            if any(word in query_lower for word in ["account", "balance", "money"]):
                entities["tables"].append("accounts")
            elif any(word in query_lower for word in ["trade", "trades", "trading"]):
                entities["tables"].append("trades")
            elif any(word in query_lower for word in ["price", "prices", "historical"]):
                entities["tables"].append("price_history")
            elif any(word in query_lower for word in ["transaction", "payment"]):
                entities["tables"].append("transactions")
            elif any(word in query_lower for word in ["order", "orders"]):
                entities["tables"].append("orders")
            else:
                entities["tables"].append("traders")

        for table in entities["tables"]:
            if table in self.schema:
                for field in self.schema[table]:
                    field_pattern = re.compile(fr'\b{field.replace("_", " ")}\b|\b{field}\b', re.IGNORECASE)
                    if field_pattern.search(query_lower):
                        entities["fields"].append(f"{table}.{field}")

        date_patterns = [
            (r'before (\d{4}-\d{2}-\d{2})', "<"),
            (r'after (\d{4}-\d{2}-\d{2})', ">"),
            (r'on (\d{4}-\d{2}-\d{2})', "="),
            (r'between (\d{4}-\d{2}-\d{2}) and (\d{4}-\d{2}-\d{2})', "between")
        ]

        for pattern, operator in date_patterns:
            matches = re.findall(pattern, query_lower)
            if matches:
                for match in matches:
                    if operator == "between":
                        entities["conditions"].append({
                            "field": self._get_date_field(entities["tables"]),
                            "operator": "between",
                            "value": match
                        })
                    else:
                        entities["conditions"].append({
                            "field": self._get_date_field(entities["tables"]),
                            "operator": operator,
                            "value": match
                        })

        numeric_patterns = [
            (r'greater than (\d+)', ">"),
            (r'less than (\d+)', "<"),
            (r'equal to (\d+)', "="),
            (r'more than (\d+)', ">"),
            (r'at least (\d+)', ">="),
            (r'at most (\d+)', "<=")
        ]

        for pattern, operator in numeric_patterns:
            matches = re.findall(pattern, query_lower)
            if matches:
                for match in matches:
                    numeric_field = self._get_numeric_field(entities["tables"], query_lower)
                    if numeric_field:
                        entities["conditions"].append({
                            "field": numeric_field,
                            "operator": operator,
                            "value": match
                        })

        limit_match = re.search(r'top (\d+)|first (\d+)|limit (\d+)', query_lower)
        if limit_match:
            limit_groups = limit_match.groups()
            for limit in limit_groups:
                if limit:
                    entities["limit"] = int(limit)
                    break

        if "highest" in query_lower or "most" in query_lower or "largest" in query_lower:
            entities["order"] = ("DESC", self._get_sort_field(entities["tables"], query_lower))
        elif "lowest" in query_lower or "least" in query_lower or "smallest" in query_lower:
            entities["order"] = ("ASC", self._get_sort_field(entities["tables"], query_lower))

        return entities

    def _get_date_field(self, tables):
        date_fields = {
            "traders": "registration_date",
            "trades": "trade_date",
            "accounts": "creation_date",
            "transactions": "transaction_date",
            "orders": "order_date",
            "price_history": "price_date",
            "order_status": "status_date"
        }

        for table in tables:
            if table in date_fields:
                return f"{table}.{date_fields[table]}"

        if tables:
            if "registration_date" in self.schema.get(tables[0], []):
                return f"{tables[0]}.registration_date"
            elif "created_at" in self.schema.get(tables[0], []):
                return f"{tables[0]}.created_at"
            elif "date" in self.schema.get(tables[0], []):
                return f"{tables[0]}.date"

        return "created_at"

    def _get_numeric_field(self, tables, query):
        numeric_fields = {
            "accounts": "balance",
            "trades": "price",
            "transactions": "amount",
            "price_history": "close_price"
        }

        if "balance" in query:
            return "accounts.balance"
        elif "price" in query:
            if "price_history" in tables:
                return "price_history.close_price"
            else:
                return "trades.price"
        elif "amount" in query:
            return "transactions.amount"
        elif "quantity" in query:
            return "trades.quantity"

        for table in tables:
            if table in numeric_fields:
                return f"{table}.{numeric_fields[table]}"

        if tables:
            if "price" in self.schema.get(tables[0], []):
                return f"{tables[0]}.price"
            elif "balance" in self.schema.get(tables[0], []):
                return f"{tables[0]}.balance"
            elif "amount" in self.schema.get(tables[0], []):
                return f"{tables[0]}.amount"

        return "id"

    def _get_sort_field(self, tables, query):
        if "balance" in query:
            return "accounts.balance"
        elif "price" in query:
            if "price_history" in tables:
                return "price_history.close_price"
            else:
                return "trades.price"
        elif "date" in query:
            return self._get_date_field(tables)
        elif "amount" in query:
            return "transactions.amount"
        elif "quantity" in query:
            return "trades.quantity"
        elif "count" in query or "number" in query:
            return "COUNT(*)"

        sort_fields = {
            "traders": "registration_date",
            "accounts": "balance",
            "trades": "trade_date",
            "transactions": "amount",
            "price_history": "close_price",
            "assets": "asset_id",
            "markets": "market_id",
            "brokers": "broker_id",
            "orders": "order_date"
        }

        for table in tables:
            if table in sort_fields:
                return f"{table}.{sort_fields[table]}"

        if tables and tables[0]:
            return f"{tables[0]}.{tables[0][:-1]}_id"

        return "id"

    def _generate_sql(self, query_type, entities, nl_query):
        if not entities["tables"]:
            return None

        main_table = entities["tables"][0]

        sql_parts = []

        is_assets_table = "assets" in entities["tables"]

        if query_type == "count":
            sql_parts.append("SELECT COUNT(*) as count")
        elif query_type.startswith("aggregate_"):
            agg_function = query_type.split("_")[1].upper()
            agg_field = None

            if "balance" in nl_query:
                agg_field = "accounts.balance"
            elif "price" in nl_query:
                agg_field = "trades.price" if "trades" in entities["tables"] else "price_history.close_price"
            elif "amount" in nl_query:
                agg_field = "transactions.amount"
            elif "date" in nl_query:
                agg_field = self._get_date_field(entities["tables"])
            else:
                if main_table == "accounts":
                    agg_field = "balance"
                elif main_table == "trades":
                    agg_field = "price"
                elif main_table == "transactions":
                    agg_field = "amount"
                elif main_table == "price_history":
                    agg_field = "close_price"
                else:
                    agg_function = "COUNT"
                    agg_field = "*"

            sql_parts.append(f"SELECT {agg_function}({agg_field}) as result")

        elif query_type == "distribution":
            group_field = None

            if "year" in nl_query:
                if "date" in nl_query:
                    date_field = self._get_date_field(entities["tables"])
                    sql_parts.append(f"SELECT YEAR({date_field}) as year, COUNT(*) as count")
                    group_field = "year"
                else:
                    sql_parts.append("SELECT YEAR(registration_date) as year, COUNT(*) as count")
                    group_field = "year"
            elif "month" in nl_query:
                date_field = self._get_date_field(entities["tables"])
                sql_parts.append(f"SELECT YEAR({date_field}) as year, MONTH({date_field}) as month, COUNT(*) as count")
                group_field = "year, month"
            elif "type" in nl_query:
                if "account" in nl_query:
                    sql_parts.append("SELECT account_type, COUNT(*) as count")
                    group_field = "account_type"
                elif "transaction" in nl_query:
                    sql_parts.append("SELECT transaction_type, COUNT(*) as count")
                    group_field = "transaction_type"
                elif "asset" in nl_query:
                    sql_parts.append("SELECT asset_type, COUNT(*) as count")
                    group_field = "asset_type"
                elif "order" in nl_query:
                    sql_parts.append("SELECT order_type, COUNT(*) as count")
                    group_field = "order_type"
                else:
                    sql_parts.append("SELECT COUNT(*) as count")
                    group_field = None
            else:
                sql_parts.append("SELECT *")
                group_field = None

            entities["group_field"] = group_field

        elif query_type == "nulls":
            null_field = None

            if "email" in nl_query:
                null_field = "email"
                sql_parts.append(
                    "SELECT COUNT(*) as total, SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_email")
            elif "phone" in nl_query:
                null_field = "phone"
                sql_parts.append(
                    "SELECT COUNT(*) as total, SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phone")
            elif "license" in nl_query:
                null_field = "license_number"
                sql_parts.append(
                    "SELECT COUNT(*) as total, SUM(CASE WHEN license_number IS NULL OR license_number = '' THEN 1 ELSE 0 END) as missing_license")
            else:
                primary_key = f"{main_table[:-1]}_id"
                related_tables = [table for table in entities["tables"] if table != main_table]

                if related_tables:
                    related_table = related_tables[0]
                    related_key = f"{related_table[:-1]}_id"

                    sql_parts.append(f"""
                        SELECT 
                            COUNT({main_table}.*) as total,
                            SUM(CASE WHEN {related_table}.{related_key} IS NULL THEN 1 ELSE 0 END) as without_related
                    """)
                else:
                    sql_parts.append("SELECT *")

        elif query_type == "existence":
            sql_parts.append("SELECT EXISTS (SELECT 1")

        elif query_type == "time":
            if "earliest" in nl_query or "oldest" in nl_query:
                date_field = self._get_date_field(entities["tables"])
                if main_table == "assets":
                    sql_parts.append("SELECT asset_id, name, asset_type, broker_id")
                else:
                    sql_parts.append("SELECT *")
                entities["order"] = ("ASC", date_field)
                entities["limit"] = 10
            elif "latest" in nl_query or "newest" in nl_query:
                date_field = self._get_date_field(entities["tables"])
                if main_table == "assets":
                    sql_parts.append("SELECT asset_id, name, asset_type, broker_id")
                else:
                    sql_parts.append("SELECT *")
                entities["order"] = ("DESC", date_field)
                entities["limit"] = 10
            else:
                if main_table == "assets":
                    sql_parts.append("SELECT asset_id, name, asset_type, broker_id")
                else:
                    sql_parts.append("SELECT *")

        else:
            if main_table == "assets":
                sql_parts.append("SELECT asset_id, name, asset_type, broker_id")
            elif entities["fields"]:
                sql_parts.append(f"SELECT {', '.join(entities['fields'])}")
            else:
                sql_parts.append("SELECT *")

        if query_type == "existence" and len(entities["tables"]) > 0:
            sql_parts.append(f"FROM {entities['tables'][0]}")
        else:
            sql_parts.append(f"FROM {main_table}")

        if len(entities["tables"]) > 1:
            for secondary_table in entities["tables"][1:]:
                join_clause = self._generate_join_clause(main_table, secondary_table)
                if join_clause:
                    sql_parts.append(join_clause)

        where_conditions = []

        for condition in entities["conditions"]:
            field = condition.get("field")
            operator = condition.get("operator")
            value = condition.get("value")

            if operator == "between" and isinstance(value, tuple) and len(value) == 2:
                where_conditions.append(f"{field} BETWEEN '{value[0]}' AND '{value[1]}'")
            elif operator in ["=", ">", "<", ">=", "<="]:
                if isinstance(value, str) and not value.isdigit():
                    where_conditions.append(f"{field} {operator} '{value}'")
                else:
                    where_conditions.append(f"{field} {operator} {value}")

        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")

        if query_type == "distribution" and "group_field" in entities and entities["group_field"]:
            sql_parts.append(f"GROUP BY {entities['group_field']}")

        if "order" in entities and entities["order"]:
            direction, field = entities["order"]
            sql_parts.append(f"ORDER BY {field} {direction}")

        if "limit" in entities:
            sql_parts.append(f"LIMIT {entities['limit']}")

        if query_type == "existence":
            sql_parts.append(") as result")

        sql = " ".join(sql_parts)

        return sql

    def _generate_join_clause(self, main_table, secondary_table):
        if (main_table, secondary_table) in self.relationships:
            main_key, sec_key = self.relationships[(main_table, secondary_table)]
            return f"JOIN {secondary_table} ON {main_table}.{main_key} = {secondary_table}.{sec_key}"

        if (secondary_table, main_table) in self.relationships:
            sec_key, main_key = self.relationships[(secondary_table, main_table)]
            return f"JOIN {secondary_table} ON {main_table}.{main_key} = {secondary_table}.{sec_key}"

        for intermediate_table in self.schema.keys():
            if intermediate_table == main_table or intermediate_table == secondary_table:
                continue

            if ((main_table, intermediate_table) in self.relationships and
                    (intermediate_table, secondary_table) in self.relationships):
                main_to_inter = self.relationships[(main_table, intermediate_table)]
                inter_to_sec = self.relationships[(intermediate_table, secondary_table)]

                return f"""
                JOIN {intermediate_table} ON {main_table}.{main_to_inter[0]} = {intermediate_table}.{main_to_inter[1]}
                JOIN {secondary_table} ON {intermediate_table}.{inter_to_sec[0]} = {secondary_table}.{inter_to_sec[1]}
                """

        main_singular = main_table[:-1] if main_table.endswith('s') else main_table
        sec_singular = secondary_table[:-1] if secondary_table.endswith('s') else secondary_table

        main_id_field = f"{main_singular}_id"
        sec_id_field = f"{sec_singular}_id"

        if main_id_field in self.schema.get(secondary_table, []):
            return f"JOIN {secondary_table} ON {main_table}.{main_singular}_id = {secondary_table}.{main_id_field}"

        if sec_id_field in self.schema.get(main_table, []):
            return f"JOIN {secondary_table} ON {main_table}.{sec_id_field} = {secondary_table}.{sec_singular}_id"

        main_id = f"{main_singular}_id"
        sec_id = f"{sec_singular}_id"
        return f"LEFT JOIN {secondary_table} ON {main_table}.{main_id} = {secondary_table}.{main_id}"

    def _execute_and_process_query(self, sql):
        try:
            result = self.db_connector.execute_query(sql)

            if not result:
                self.logger.info("No results returned from database")
                return []

            processed_result = []
            for item in result:
                processed_item = {}
                for key, value in item.items():
                    if key.endswith('_encrypted'):
                        continue

                    if self._is_sensitive_field(key):
                        processed_item[key] = value
                    else:
                        processed_item[key] = value

                processed_result.append(processed_item)

            return processed_result

        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return None

    def _is_sensitive_field(self, field_name):
        if '.' in field_name:
            table, field = field_name.split('.')
            return (table in self.sensitive_fields and
                    field in self.sensitive_fields.get(table, []))

        return field_name in self.sensitive_fields


    def get_traders_by_registration_date(self, date, operator="<"):
        sql = f"""
        SELECT * FROM traders
        WHERE registration_date {operator} '{date}'
        ORDER BY registration_date
        """
        return self._execute_and_process_query(sql)

    def get_trader_count(self):
        sql = "SELECT COUNT(*) as count FROM traders"
        return self._execute_and_process_query(sql)

    def get_traders_without_contact(self, contact_field="email"):
        sql = f"""
        SELECT * FROM traders
        WHERE {contact_field} IS NULL OR {contact_field} = ''
        """
        return self._execute_and_process_query(sql)

    def get_common_email_domains(self, limit=5):
        sql = f"""
        SELECT 
            SUBSTRING_INDEX(email, '@', -1) as domain,
            COUNT(*) as count
        FROM traders
        WHERE email IS NOT NULL AND email != ''
        GROUP BY domain
        ORDER BY count DESC
        LIMIT {limit}
        """
        return self._execute_and_process_query(sql)

    def get_traders_with_same_name(self):
        sql = """
        SELECT name, COUNT(*) as count
        FROM traders
        GROUP BY name
        HAVING count > 1
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_oldest_traders(self, limit=10):
        sql = f"""
        SELECT * FROM traders
        ORDER BY registration_date ASC
        LIMIT {limit}
        """
        return self._execute_and_process_query(sql)

    def get_traders_without_trades(self):
        sql = """
        SELECT t.*
        FROM traders t
        LEFT JOIN trades tr ON t.trader_id = tr.trader_id
        WHERE tr.trade_id IS NULL
        """
        return self._execute_and_process_query(sql)

    def get_traders_with_multiple_accounts(self):
        sql = """
        SELECT 
            t.trader_id,
            t.name,
            COUNT(a.account_id) as account_count
        FROM traders t
        JOIN accounts a ON t.trader_id = a.trader_id
        GROUP BY t.trader_id, t.name
        HAVING account_count > 1
        ORDER BY account_count DESC
        """
        return self._execute_and_process_query(sql)

    def get_average_registration_date(self):
        sql = """
        SELECT 
            AVG(UNIX_TIMESTAMP(registration_date)) as avg_timestamp,
            FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(registration_date))) as avg_date
        FROM traders
        """
        return self._execute_and_process_query(sql)

    def get_trader_registration_distribution(self):
        sql = """
        SELECT 
            YEAR(registration_date) as year,
            COUNT(*) as count
        FROM traders
        GROUP BY year
        ORDER BY year
        """
        return self._execute_and_process_query(sql)

    def get_market_count(self):
        sql = "SELECT COUNT(*) as count FROM markets"
        return self._execute_and_process_query(sql)

    def get_market_trading_hours(self):
        sql = """
        SELECT 
            name, 
            opening_time, 
            closing_time,
            TIMEDIFF(closing_time, opening_time) as trading_duration
        FROM markets
        ORDER BY opening_time
        """
        return self._execute_and_process_query(sql)

    def get_earliest_latest_markets(self):
        sql = """
        SELECT 
            (SELECT name FROM markets ORDER BY opening_time ASC LIMIT 1) as earliest_opening,
            (SELECT opening_time FROM markets ORDER BY opening_time ASC LIMIT 1) as earliest_time,
            (SELECT name FROM markets ORDER BY closing_time DESC LIMIT 1) as latest_closing,
            (SELECT closing_time FROM markets ORDER BY closing_time DESC LIMIT 1) as latest_time
        """
        return self._execute_and_process_query(sql)

    def get_overlapping_markets(self):
        sql = """
        SELECT 
            m1.name as market1, 
            m2.name as market2,
            m1.opening_time as m1_open,
            m1.closing_time as m1_close,
            m2.opening_time as m2_open,
            m2.closing_time as m2_close
        FROM markets m1
        JOIN markets m2 ON m1.market_id < m2.market_id
        WHERE 
            (m1.opening_time BETWEEN m2.opening_time AND m2.closing_time) OR
            (m1.closing_time BETWEEN m2.opening_time AND m2.closing_time) OR
            (m2.opening_time BETWEEN m1.opening_time AND m1.closing_time) OR
            (m2.closing_time BETWEEN m1.opening_time AND m1.closing_time)
        """
        return self._execute_and_process_query(sql)

    def get_markets_by_location(self):
        sql = """
        SELECT 
            location,
            COUNT(*) as count
        FROM markets
        GROUP BY location
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_duplicate_market_names(self):
        sql = """
        SELECT 
            name,
            COUNT(*) as name_count
        FROM markets
        GROUP BY name
        HAVING name_count > 1
        """
        return self._execute_and_process_query(sql)

    def get_busiest_market(self):
        sql = """
        SELECT 
            m.market_id,
            m.name,
            COUNT(t.trade_id) as trade_count
        FROM markets m
        LEFT JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.market_id, m.name
        ORDER BY trade_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_broker_count(self):
        sql = "SELECT COUNT(*) as count FROM brokers"
        return self._execute_and_process_query(sql)

    def get_brokers_without_license(self):
        sql = """
        SELECT * FROM brokers
        WHERE license_number IS NULL OR license_number = ''
        """
        return self._execute_and_process_query(sql)

    def get_brokers_with_same_email(self):
        sql = """
        SELECT contact_email, COUNT(*) as count
        FROM brokers
        WHERE contact_email IS NOT NULL AND contact_email != ''
        GROUP BY contact_email
        HAVING count > 1
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_broker_with_most_assets(self):
        sql = """
        SELECT 
            b.broker_id,
            b.name,
            COUNT(a.asset_id) as asset_count
        FROM brokers b
        LEFT JOIN assets a ON b.broker_id = a.broker_id
        GROUP BY b.broker_id, b.name
        ORDER BY asset_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_average_assets_per_broker(self):
        sql = """
        SELECT 
            COUNT(a.asset_id) / COUNT(DISTINCT b.broker_id) as avg_assets_per_broker
        FROM brokers b
        LEFT JOIN assets a ON b.broker_id = a.broker_id
        """
        return self._execute_and_process_query(sql)

    def get_brokers_without_assets(self):
        sql = """
        SELECT b.*
        FROM brokers b
        LEFT JOIN assets a ON b.broker_id = a.broker_id
        WHERE a.asset_id IS NULL
        """
        return self._execute_and_process_query(sql)

    def get_asset_count(self):
        sql = "SELECT COUNT(*) as count FROM assets"
        return self._execute_and_process_query(sql)

    def get_asset_types(self):
        sql = """
        SELECT 
            asset_type,
            COUNT(*) as count
        FROM assets
        GROUP BY asset_type
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_most_traded_asset(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            a.asset_type,
            COUNT(t.trade_id) as trade_count
        FROM assets a
        LEFT JOIN trades t ON a.asset_id = t.asset_id
        GROUP BY a.asset_id, a.name, a.asset_type
        ORDER BY trade_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_assets_without_broker(self):
        sql = """
               SELECT asset_id, name, asset_type, broker_id
               FROM assets
               WHERE broker_id IS NULL
               """
        return self._execute_and_process_query(sql)


        def get_actively_traded_assets(self):
            sql = """
            SELECT
                a.asset_id, a.name, a.asset_type, a.broker_id,
                COUNT(t.trade_id) as trade_count
            FROM assets a
            JOIN trades t ON a.asset_id = t.asset_id
            WHERE t.trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY a.asset_id, a.name, a.asset_type, a.broker_id, a.api_symbol
            ORDER BY trade_count DESC
            """
            return self._execute_and_process_query(sql)

    def get_average_trades_per_asset(self):
        sql = """
        SELECT 
            COUNT(t.trade_id) / COUNT(DISTINCT a.asset_id) as avg_trades_per_asset
        FROM assets a
        LEFT JOIN trades t ON a.asset_id = t.asset_id
        """
        return self._execute_and_process_query(sql)

    def get_trade_count(self):
        sql = "SELECT COUNT(*) as count FROM trades"
        return self._execute_and_process_query(sql)

    def get_highest_trade_quantity(self):
        sql = """
        SELECT 
            t.*,
            a.name as asset_name,
            tr.name as trader_name
        FROM trades t
        JOIN assets a ON t.asset_id = a.asset_id
        JOIN traders tr ON t.trader_id = tr.trader_id
        ORDER BY t.quantity DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_trade_price_range(self):
        sql = """
        SELECT 
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price
        FROM trades
        """
        return self._execute_and_process_query(sql)

    def get_trader_most_trades(self):
        sql = """
        SELECT 
            tr.trader_id,
            tr.name,
            COUNT(t.trade_id) as trade_count
        FROM traders tr
        JOIN trades t ON tr.trader_id = t.trader_id
        GROUP BY tr.trader_id, tr.name
        ORDER BY trade_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_average_trade_quantity(self):
        sql = "SELECT AVG(quantity) as avg_quantity FROM trades"
        return self._execute_and_process_query(sql)

    def get_trades_with_nulls(self):
        sql = """
        SELECT 
            COUNT(*) as total_trades,
            SUM(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) as missing_asset,
            SUM(CASE WHEN trader_id IS NULL THEN 1 ELSE 0 END) as missing_trader,
            SUM(CASE WHEN market_id IS NULL THEN 1 ELSE 0 END) as missing_market
        FROM trades
        """
        return self._execute_and_process_query(sql)

    def get_market_trade_activity(self):
        sql = """
        SELECT 
            m.market_id,
            m.name,
            COUNT(t.trade_id) as trade_count
        FROM markets m
        LEFT JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.market_id, m.name
        ORDER BY trade_count DESC
        """
        return self._execute_and_process_query(sql)

    def get_average_open_price(self):
        sql = "SELECT AVG(open_price) as avg_open_price FROM price_history"
        return self._execute_and_process_query(sql)

    def get_price_records_per_asset(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            COUNT(p.price_id) as price_record_count
        FROM assets a
        LEFT JOIN price_history p ON a.asset_id = p.asset_id
        GROUP BY a.asset_id, a.name
        ORDER BY price_record_count DESC
        """
        return self._execute_and_process_query(sql)

    def get_highest_price_fluctuation(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            p.price_date,
            p.open_price,
            p.close_price,
            ABS(p.close_price - p.open_price) as price_change,
            (ABS(p.close_price - p.open_price) / p.open_price * 100) as percentage_change
        FROM price_history p
        JOIN assets a ON p.asset_id = a.asset_id
        WHERE p.open_price > 0
        ORDER BY percentage_change DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_earliest_price_record(self):
        sql = """
        SELECT 
            p.*,
            a.name as asset_name
        FROM price_history p
        JOIN assets a ON p.asset_id = a.asset_id
        ORDER BY p.price_date ASC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_orphaned_price_records(self):
        sql = """
        SELECT 
            p.*,
            a.name as asset_name
        FROM price_history p
        JOIN assets a ON p.asset_id = a.asset_id
        LEFT JOIN trades t ON p.asset_id = t.asset_id
        WHERE t.trade_id IS NULL
        """
        return self._execute_and_process_query(sql)

    def get_latest_prices(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            p.price_date,
            p.close_price
        FROM assets a
        JOIN (
            SELECT asset_id, MAX(price_date) as max_date
            FROM price_history
            GROUP BY asset_id
        ) latest ON a.asset_id = latest.asset_id
        JOIN price_history p ON latest.asset_id = p.asset_id AND latest.max_date = p.price_date
        """
        return self._execute_and_process_query(sql)

    def get_account_count(self):
        sql = "SELECT COUNT(*) as count FROM accounts"
        return self._execute_and_process_query(sql)

    def get_average_account_balance(self):
        sql = """
        SELECT 
            AVG(balance) as avg_balance,
            MIN(balance) as min_balance,
            MAX(balance) as max_balance,
            SUM(balance) as total_balance
        FROM accounts
        """
        return self._execute_and_process_query(sql)

    def get_non_positive_balance_accounts(self):
        sql = """
        SELECT 
            COUNT(*) as total_accounts,
            SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END) as negative_balance_count,
            SUM(CASE WHEN balance = 0 THEN 1 ELSE 0 END) as zero_balance_count,
            SUM(CASE WHEN balance <= 0 THEN 1 ELSE 0 END) as non_positive_balance_count
        FROM accounts
        """
        return self._execute_and_process_query(sql)

    def get_highest_balance_account(self):
        sql = """
        SELECT 
            a.account_id,
            a.balance,
            a.account_type,
            t.trader_id,
            t.name as trader_name
        FROM accounts a
        JOIN traders t ON a.trader_id = t.trader_id
        ORDER BY a.balance DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_account_types(self):
        sql = """
        SELECT 
            account_type,
            COUNT(*) as count,
            AVG(balance) as avg_balance
        FROM accounts
        GROUP BY account_type
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_accounts_before_date(self, date):
        sql = f"""
        SELECT 
            a.*,
            t.name as trader_name
        FROM accounts a
        JOIN traders t ON a.trader_id = t.trader_id
        WHERE a.creation_date < '{date}'
        ORDER BY a.creation_date
        """
        return self._execute_and_process_query(sql)

    def get_accounts_per_trader(self):
        sql = """
        SELECT 
            t.trader_id,
            t.name,
            COUNT(a.account_id) as account_count
        FROM traders t
        LEFT JOIN accounts a ON t.trader_id = a.trader_id
        GROUP BY t.trader_id, t.name
        ORDER BY account_count DESC
        """
        return self._execute_and_process_query(sql)

    def get_transaction_count(self):
        sql = "SELECT COUNT(*) as count FROM transactions"
        return self._execute_and_process_query(sql)

    def get_transaction_types(self):
        sql = """
        SELECT 
            transaction_type,
            COUNT(*) as count
        FROM transactions
        GROUP BY transaction_type
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_transaction_amount_range(self):
        sql = """
        SELECT 
            MIN(amount) as min_amount,
            MAX(amount) as max_amount,
            AVG(amount) as avg_amount,
            SUM(amount) as total_amount
        FROM transactions
        """
        return self._execute_and_process_query(sql)

    def get_account_with_most_transactions(self):
        sql = """
        SELECT 
            a.account_id,
            a.account_type,
            t.name as trader_name,
            COUNT(tr.transaction_id) as transaction_count,
            SUM(tr.amount) as total_amount
        FROM accounts a
        JOIN traders t ON a.trader_id = t.trader_id
        JOIN transactions tr ON a.account_id = tr.account_id
        GROUP BY a.account_id, a.account_type, t.name
        ORDER BY transaction_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_accounts_without_transactions(self):
        sql = """
        SELECT 
            a.*,
            t.name as trader_name
        FROM accounts a
        JOIN traders t ON a.trader_id = t.trader_id
        LEFT JOIN transactions tr ON a.account_id = tr.account_id
        WHERE tr.transaction_id IS NULL
        """
        return self._execute_and_process_query(sql)

    def get_transactions_on_date(self, date):
        sql = f"""
        SELECT 
            tr.*,
            a.account_type,
            t.name as trader_name
        FROM transactions tr
        JOIN accounts a ON tr.account_id = a.account_id
        JOIN traders t ON a.trader_id = t.trader_id
        WHERE DATE(tr.transaction_date) = '{date}'
        ORDER BY tr.transaction_date
        """
        return self._execute_and_process_query(sql)

    def get_total_transacted_amount(self):
        sql = """
        SELECT 
            SUM(ABS(amount)) as total_gross_amount,
            SUM(amount) as total_net_amount
        FROM transactions
        """
        return self._execute_and_process_query(sql)

    def get_order_count(self):
        sql = "SELECT COUNT(*) as count FROM orders"
        return self._execute_and_process_query(sql)

    def get_order_types(self):
        sql = """
        SELECT 
            order_type,
            COUNT(*) as count
        FROM orders
        GROUP BY order_type
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_trades_with_most_orders(self):
        sql = """
        SELECT 
            t.trade_id,
            t.trade_date,
            a.name as asset_name,
            tr.name as trader_name,
            COUNT(o.order_id) as order_count
        FROM trades t
        JOIN assets a ON t.asset_id = a.asset_id
        JOIN traders tr ON t.trader_id = tr.trader_id
        LEFT JOIN orders o ON t.trade_id = o.trade_id
        GROUP BY t.trade_id, t.trade_date, a.name, tr.name
        ORDER BY order_count DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_orders_on_date(self, date):
        sql = f"""
        SELECT 
            o.*,
            t.trade_date,
            a.name as asset_name,
            tr.name as trader_name
        FROM orders o
        JOIN trades t ON o.trade_id = t.trade_id
        JOIN assets a ON t.asset_id = a.asset_id
        JOIN traders tr ON t.trader_id = tr.trader_id
        WHERE DATE(o.order_date) = '{date}'
        ORDER BY o.order_date
        """
        return self._execute_and_process_query(sql)

    def get_orphaned_orders(self):
        sql = """
        SELECT 
            o.*
        FROM orders o
        LEFT JOIN trades t ON o.trade_id = t.trade_id
        WHERE t.trade_id IS NULL
        """
        return self._execute_and_process_query(sql)

    def get_order_statuses(self):
        sql = """
        SELECT 
            status,
            COUNT(*) as count
        FROM order_status
        GROUP BY status
        ORDER BY count DESC
        """
        return self._execute_and_process_query(sql)

    def get_orders_by_status(self, status):
        sql = f"""
        SELECT 
            o.*,
            os.status,
            os.status_date,
            t.trade_date
        FROM orders o
        JOIN order_status os ON o.order_id = os.order_id
        JOIN trades t ON o.trade_id = t.trade_id
        WHERE os.status = '{status}'
        ORDER BY os.status_date DESC
        """
        return self._execute_and_process_query(sql)

    def get_slow_completion_orders(self):
        sql = """
        SELECT 
            o.order_id,
            o.order_type,
            o.order_date,
            completed.status_date as completion_date,
            TIMESTAMPDIFF(HOUR, o.order_date, completed.status_date) as hours_to_complete
        FROM orders o
        JOIN (
            SELECT order_id, status_date
            FROM order_status
            WHERE status = 'Completed'
        ) completed ON o.order_id = completed.order_id
        ORDER BY hours_to_complete DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def get_avg_status_change_time(self):
        sql = """
        SELECT 
            os.status,
            AVG(TIMESTAMPDIFF(MINUTE, o.order_date, os.status_date)) as avg_minutes_to_status
        FROM orders o
        JOIN order_status os ON o.order_id = os.order_id
        GROUP BY os.status
        ORDER BY avg_minutes_to_status DESC
        """
        return self._execute_and_process_query(sql)

    def get_orders_with_multiple_statuses(self):
        sql = """
        SELECT 
            o.order_id,
            o.order_type,
            o.order_date,
            COUNT(os.status_id) as status_count
        FROM orders o
        JOIN order_status os ON o.order_id = os.order_id
        GROUP BY o.order_id, o.order_type, o.order_date
        HAVING status_count > 1
        ORDER BY status_count DESC
        """
        return self._execute_and_process_query(sql)


    def analyze_trader_activity_over_time(self):
        sql = """
        SELECT 
            DATE_FORMAT(trade_date, '%Y-%m') as month,
            COUNT(DISTINCT trader_id) as active_traders,
            COUNT(trade_id) as total_trades,
            SUM(quantity * price) as total_value
        FROM trades
        GROUP BY month
        ORDER BY month
        """
        return self._execute_and_process_query(sql)

    def analyze_market_correlations(self):
        sql = """
        SELECT 
            m1.name as market1,
            m2.name as market2,
            COUNT(t1.trade_id) as market1_trades,
            COUNT(t2.trade_id) as market2_trades,
            COUNT(DISTINCT t1.trader_id) as common_traders
        FROM markets m1
        JOIN markets m2 ON m1.market_id < m2.market_id
        JOIN trades t1 ON m1.market_id = t1.market_id
        JOIN trades t2 ON m2.market_id = t2.market_id AND t1.trader_id = t2.trader_id
        GROUP BY m1.name, m2.name
        ORDER BY common_traders DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def analyze_asset_price_volatility(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            a.asset_type,
            COUNT(p.price_id) as price_points,
            AVG(p.close_price) as avg_price,
            MAX(p.close_price) - MIN(p.close_price) as price_range,
            STDDEV(p.close_price) as price_stddev,
            (STDDEV(p.close_price) / AVG(p.close_price) * 100) as coefficient_of_variation
        FROM assets a
        JOIN price_history p ON a.asset_id = p.asset_id
        GROUP BY a.asset_id, a.name, a.asset_type
        HAVING price_points > 5
        ORDER BY coefficient_of_variation DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def analyze_trader_portfolio_diversity(self):
        sql = """
        SELECT 
            tr.trader_id,
            tr.name,
            COUNT(DISTINCT t.asset_id) as unique_assets,
            COUNT(DISTINCT a.asset_type) as unique_asset_types,
            COUNT(t.trade_id) as total_trades
        FROM traders tr
        JOIN trades t ON tr.trader_id = t.trader_id
        JOIN assets a ON t.asset_id = a.asset_id
        GROUP BY tr.trader_id, tr.name
        ORDER BY unique_assets DESC, unique_asset_types DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def analyze_transaction_patterns(self):
        sql = """
        SELECT 
            HOUR(transaction_date) as hour_of_day,
            DAYNAME(transaction_date) as day_of_week,
            COUNT(*) as transaction_count,
            AVG(amount) as avg_amount,
            SUM(amount) as total_amount
        FROM transactions
        GROUP BY hour_of_day, day_of_week
        ORDER BY transaction_count DESC
        """
        return self._execute_and_process_query(sql)

    def analyze_account_type_performance(self):
        sql = """
        SELECT 
            a.account_type,
            COUNT(a.account_id) as num_accounts,
            AVG(a.balance) as avg_balance,
            SUM(a.balance) as total_balance,
            COUNT(t.trade_id) as total_trades,
            COUNT(t.trade_id) / COUNT(a.account_id) as trades_per_account
        FROM accounts a
        LEFT JOIN traders tr ON a.trader_id = tr.trader_id
        LEFT JOIN trades t ON tr.trader_id = t.trader_id
        GROUP BY a.account_type
        ORDER BY avg_balance DESC
        """
        return self._execute_and_process_query(sql)

    def analyze_order_completion_efficiency(self):
        sql = """
        SELECT 
            o.order_type,
            COUNT(o.order_id) as total_orders,
            AVG(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as avg_completion_time,
            MIN(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as min_completion_time,
            MAX(TIMESTAMPDIFF(MINUTE, o.order_date, completed.status_date)) as max_completion_time
        FROM orders o
        JOIN (
            SELECT order_id, status_date
            FROM order_status
            WHERE status = 'Completed'
        ) completed ON o.order_id = completed.order_id
        GROUP BY o.order_type
        ORDER BY avg_completion_time
        """
        return self._execute_and_process_query(sql)

    def analyze_broker_asset_distribution(self):
        sql = """
        SELECT 
            b.broker_id,
            b.name,
            COUNT(a.asset_id) as total_assets,
            SUM(CASE WHEN a.asset_type = 'Stock' THEN 1 ELSE 0 END) as stocks,
            SUM(CASE WHEN a.asset_type = 'ETF' THEN 1 ELSE 0 END) as etfs,
            SUM(CASE WHEN a.asset_type = 'Bond' THEN 1 ELSE 0 END) as bonds,
            SUM(CASE WHEN a.asset_type = 'Cryptocurrency' THEN 1 ELSE 0 END) as crypto,
            SUM(CASE WHEN a.asset_type = 'Commodity' THEN 1 ELSE 0 END) as commodities,
            SUM(CASE WHEN a.asset_type = 'Options' THEN 1 ELSE 0 END) as options,
            SUM(CASE WHEN a.asset_type = 'Futures' THEN 1 ELSE 0 END) as futures
        FROM brokers b
        LEFT JOIN assets a ON b.broker_id = a.broker_id
        GROUP BY b.broker_id, b.name
        ORDER BY total_assets DESC
        """
        return self._execute_and_process_query(sql)

    def identify_anomalous_trading_activity(self):
        sql = """
        SELECT 
            tr.trader_id,
            tr.name,
            t.trade_id,
            t.trade_date,
            a.name as asset_name,
            t.quantity,
            t.price,
            (t.quantity * t.price) as trade_value
        FROM trades t
        JOIN traders tr ON t.trader_id = tr.trader_id
        JOIN assets a ON t.asset_id = a.asset_id
        WHERE (t.quantity * t.price) > (
            SELECT AVG(quantity * price) + 3 * STDDEV(quantity * price)
            FROM trades
        )
        ORDER BY trade_value DESC
        LIMIT 20
        """
        return self._execute_and_process_query(sql)

    def analyze_market_peak_hours(self):
        sql = """
        SELECT 
            m.name as market_name,
            HOUR(t.trade_date) as hour_of_day,
            COUNT(t.trade_id) as trade_count,
            SUM(t.quantity * t.price) as total_value
        FROM markets m
        JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.name, hour_of_day
        ORDER BY m.name, trade_count DESC
        """
        return self._execute_and_process_query(sql)

    def identify_market_inefficiencies(self):
        sql = """
        SELECT 
            a.asset_id,
            a.name,
            a.asset_type,
            m1.name as market1,
            m2.name as market2,
            AVG(t1.price) as avg_price_market1,
            AVG(t2.price) as avg_price_market2,
            ABS(AVG(t1.price) - AVG(t2.price)) as price_difference,
            (ABS(AVG(t1.price) - AVG(t2.price)) / GREATEST(AVG(t1.price), AVG(t2.price)) * 100) as percentage_difference
        FROM assets a
        JOIN trades t1 ON a.asset_id = t1.asset_id
        JOIN trades t2 ON a.asset_id = t2.asset_id AND t1.market_id < t2.market_id
        JOIN markets m1 ON t1.market_id = m1.market_id
        JOIN markets m2 ON t2.market_id = m2.market_id
        WHERE t1.trade_date BETWEEN DATE_SUB(NOW(), INTERVAL 7 DAY) AND NOW()
        AND t2.trade_date BETWEEN DATE_SUB(NOW(), INTERVAL 7 DAY) AND NOW()
        GROUP BY a.asset_id, a.name, a.asset_type, m1.name, m2.name
        HAVING percentage_difference > 1
        ORDER BY percentage_difference DESC
        LIMIT 10
        """
        return self._execute_and_process_query(sql)

    def analyze_data_quality_issues(self):
        sql = """
        SELECT 
            'traders' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
            SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_emails,
            SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phones,
            SUM(CASE WHEN registration_date IS NULL THEN 1 ELSE 0 END) as missing_dates
        FROM traders

        UNION ALL

        SELECT 
            'accounts' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN trader_id IS NULL THEN 1 ELSE 0 END) as missing_trader_id,
            SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END) as missing_balance,
            SUM(CASE WHEN account_type IS NULL OR account_type = '' THEN 1 ELSE 0 END) as missing_account_type,
            SUM(CASE WHEN creation_date IS NULL THEN 1 ELSE 0 END) as missing_creation_date
        FROM accounts

        UNION ALL

        SELECT 
            'trades' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN trader_id IS NULL THEN 1 ELSE 0 END) as missing_trader_id,
            SUM(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) as missing_asset_id,
            SUM(CASE WHEN market_id IS NULL THEN 1 ELSE 0 END) as missing_market_id,
            SUM(CASE WHEN trade_date IS NULL THEN 1 ELSE 0 END) as missing_trade_date
        FROM trades

        UNION ALL

        SELECT 
            'assets' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) as missing_names,
            SUM(CASE WHEN asset_type IS NULL OR asset_type = '' THEN 1 ELSE 0 END) as missing_asset_type,
            SUM(CASE WHEN broker_id IS NULL THEN 1 ELSE 0 END) as missing_broker_id,
            0 as placeholder
        FROM assets
        """
        return self._execute_and_process_query(sql)