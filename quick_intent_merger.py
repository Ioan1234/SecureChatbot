import logging
import re


class QuickIntentMerger:

    def __init__(self, intent_classifier):
        self.logger = logging.getLogger(__name__)
        self.classifier = intent_classifier

        self.merge_mappings = {
            "database_query_comparative_highest": "database_query_comparative",
            "database_query_comparative_lowest": "database_query_comparative",
            "database_query_comparative_middle": "database_query_comparative",

            "database_query_sort_ascending": "database_query_sort",
            "database_query_sort_descending": "database_query_sort"
        }

        self.comparative_indicators = {
            "highest": ["highest", "maximum", "most", "top", "greatest", "largest", "biggest", "best", "max",
                        "highest-rated"],
            "lowest": ["lowest", "minimum", "least", "bottom", "smallest", "cheapest", "worst", "min", "lowest-rated"],
            "middle": ["middle", "median", "average", "mid", "center", "mean", "medium", "intermediate", "avg"]
        }

        self.sort_indicators = {
            "ascending": ["ascending", "increasing", "low to high", "smallest to largest",
                          "least to most", "lowest to highest", "asc", "up"],
            "descending": ["descending", "decreasing", "high to low", "largest to smallest",
                           "most to least", "highest to lowest", "desc", "down"]
        }

        self._compile_intent_patterns()

    def _compile_intent_patterns(self):
        self.intent_patterns = {
            r'\b(sort|order|arrange).*(descending|desc|high to low|largest to smallest)': {
                "intent": "database_query_sort",
                "sub_intent": "database_query_sort_descending"
            },
            r'\b(sort|order|arrange).*(ascending|asc|low to high|smallest to largest)': {
                "intent": "database_query_sort",
                "sub_intent": "database_query_sort_ascending"
            },
            r'\b(sort|order|arrange|list).*\b(by|on)\b': {
                "intent": "database_query_sort",
            },

            r'\b(highest|maximum|most|top|greatest|largest|biggest)': {
                "intent": "database_query_comparative",
                "sub_intent": "database_query_comparative_highest"
            },
            r'\b(lowest|minimum|least|bottom|smallest|cheapest|worst)': {
                "intent": "database_query_comparative",
                "sub_intent": "database_query_comparative_lowest"
            },
            r'\b(median|middle|average|mid|center|mean)': {
                "intent": "database_query_comparative",
                "sub_intent": "database_query_comparative_middle"
            },

            r'\b(show|display|list|get|find|retrieve)\b.*\b(all|every)\b': {
                "intent": "database_query_list"
            },

            r'\b(all details|more details|detailed|complete|all information|everything about|all data|show all|all fields|full record)': {
                "intent": "database_query_detailed"
            },

            r'\b(count|how many|number of|total number)': {
                "intent": "database_query_count"
            },

            r'\b(recent|latest|newest|current|fresh|just added|new)': {
                "intent": "database_query_recent"
            },

            r'\b(where|with|having|that have|filter)\b': {
                "intent": "database_query_filter"
            },

            r'\b(with id|number|#)\s*(\d+)': {
                "intent": "database_query_specific_id"
            },

            r'\b(encrypted|sensitive|private|personal|email|contact|license|phone)': {
                "intent": "database_query_sensitive"
            },
            r'\b(find|show|list|get|display)\b.*\btraders?\b.*\b(with|having)\b.*\b(highest|most|largest|biggest|maximum)\b.*\b(balance|account balance)\b': {
                "intent": "database_query_comparative",
                "sub_intent": "database_query_comparative_highest",
                "entity": "traders",
                "attribute": "balance"
            }

        }
        asset_types = [
            r'stocks?', r'bonds?', r'etfs?', r'cryptocurrenc(?:y|ies)', r'commodit(?:y|ies)',
            r'options?', r'futures?', r'forex', r'reits?'
        ]

        for asset_type in asset_types:
            pattern = rf'\b(show|get|list|display)\b.*\b(all)?\b.*\b{asset_type}\b'
            self.intent_patterns[pattern] = {
                "intent": "database_query_asset_type",
                "asset_type": asset_type.rstrip('s?')
            }

            pattern2 = rf'\b{asset_type}\b.*\bassets?\b'
            self.intent_patterns[pattern2] = {
                "intent": "database_query_asset_type",
                "asset_type": asset_type.rstrip('s?')
            }
        self.compiled_patterns = {
            re.compile(pattern, re.IGNORECASE): info
            for pattern, info in self.intent_patterns.items()
        }

        self.compiled_patterns = {
            re.compile(pattern, re.IGNORECASE): info
            for pattern, info in self.intent_patterns.items()
        }

    def classify_intent(self, query):
        result = self.classifier.classify_intent(query)

        if not result or not isinstance(result, dict):
            return {"intent": "database_query_list", "confidence": 0.5}

        intent = result.get("intent", "")
        confidence = result.get("confidence", 0.0)

        self.logger.info(f"Original ML classification: {intent} with confidence {confidence}")

        query_lower = query.lower()
        pattern_matches = []

        for compiled_pattern, intent_info in self.compiled_patterns.items():
            if compiled_pattern.search(query_lower):
                pattern_matches.append((compiled_pattern.pattern, intent_info))

        if pattern_matches:
            self.logger.info(f"Pattern matches: {[m[0] for m in pattern_matches]}")

            if confidence < 0.8:
                pattern_info = pattern_matches[0][1]
                self.logger.info(f"Low ML confidence ({confidence}). Using pattern match: {pattern_info}")

                new_intent = pattern_info["intent"]
                sub_intent = pattern_info.get("sub_intent")

                result = {
                    "intent": new_intent,
                    "confidence": 0.9
                }

                if sub_intent:
                    result["sub_intent"] = sub_intent

        if "intent" in result:
            intent = result["intent"]

            if intent in self.merge_mappings:
                parent_intent = self.merge_mappings[intent]

                result["parent_intent"] = parent_intent
                result["sub_intent"] = intent
                result["original_intent"] = intent
                result["intent"] = parent_intent

            elif "sub_intent" not in result:
                if intent == "database_query_sort":
                    result["sub_intent"] = self._detect_sort_sub_intent(query_lower)

                elif intent == "database_query_comparative":
                    result["sub_intent"] = self._detect_comparative_sub_intent(query_lower)

        self.logger.info(f"Final intent classification: {result}")
        return result

    def _detect_sort_sub_intent(self, query_lower):
        for indicator in self.sort_indicators["descending"]:
            if indicator in query_lower:
                return "database_query_sort_descending"
        for indicator in self.sort_indicators["ascending"]:
            if indicator in query_lower:
                return "database_query_sort_ascending"
        return "database_query_sort_ascending"

    def _detect_comparative_sub_intent(self, query_lower):
        for indicator in self.comparative_indicators["highest"]:
            if indicator in query_lower:
                return "database_query_comparative_highest"

        for indicator in self.comparative_indicators["lowest"]:
            if indicator in query_lower:
                return "database_query_comparative_lowest"

        for indicator in self.comparative_indicators["middle"]:
            if indicator in query_lower:
                return "database_query_comparative_middle"

        return "database_query_comparative_highest"