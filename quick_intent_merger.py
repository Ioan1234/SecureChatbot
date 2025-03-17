import logging


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
            "highest": ["highest", "maximum", "most", "top", "greatest", "largest", "biggest", "best"],
            "lowest": ["lowest", "minimum", "least", "bottom", "smallest", "cheapest", "worst"],
            "middle": ["middle", "median", "average", "mid", "center", "mean", "medium"]
        }

        self.sort_indicators = {
            "ascending": ["ascending", "increasing", "low to high", "smallest to largest", "least to most", "asc",
                          "up"],
            "descending": ["descending", "decreasing", "high to low", "largest to smallest", "most to least", "desc",
                           "down"]
        }

    def classify_intent(self, query):

        result = self.classifier.classify_intent(query)

        if not result or not isinstance(result, dict):
            return {"intent": "database_query_list", "confidence": 0.5}

        intent = result.get("intent", "")
        confidence = result.get("confidence", 0.0)

        if intent in self.merge_mappings.keys():
            parent_intent = self.merge_mappings[intent]

            result["parent_intent"] = parent_intent
            result["sub_intent"] = intent

            result["original_intent"] = intent
            result["intent"] = parent_intent

        elif intent == "database_query_sort":
            query_lower = query.lower()

            sub_intent = None

            for indicator in self.sort_indicators["ascending"]:
                if indicator in query_lower:
                    sub_intent = "database_query_sort_ascending"
                    break

            if not sub_intent:
                for indicator in self.sort_indicators["descending"]:
                    if indicator in query_lower:
                        sub_intent = "database_query_sort_descending"
                        break

            if not sub_intent:
                sub_intent = "database_query_sort_ascending"

            result["sub_intent"] = sub_intent

        elif intent == "database_query_comparative":
            query_lower = query.lower()

            sub_intent = None

            for indicator in self.comparative_indicators["highest"]:
                if indicator in query_lower:
                    sub_intent = "database_query_comparative_highest"
                    break

            if not sub_intent:
                for indicator in self.comparative_indicators["lowest"]:
                    if indicator in query_lower:
                        sub_intent = "database_query_comparative_lowest"
                        break

            if not sub_intent:
                for indicator in self.comparative_indicators["middle"]:
                    if indicator in query_lower:
                        sub_intent = "database_query_comparative_middle"
                        break

            if not sub_intent:
                sub_intent = "database_query_comparative_highest"

            result["sub_intent"] = sub_intent

        return result
