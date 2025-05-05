import os
import json
import logging
import re


class IntentPostProcessor:
    

    def __init__(self, model_dir, use_fallback=True):
        
        self.logger = logging.getLogger(__name__)
        self.mappings = {}
        self.original_examples = {}
        self.use_fallback = use_fallback

        mappings_path = os.path.join(model_dir, "intent_mappings.json")
        if os.path.exists(mappings_path):
            try:
                with open(mappings_path, 'r') as f:
                    data = json.load(f)
                    self.mappings = data.get("merge_mappings", {})
                self.logger.info(f"Loaded {len(self.mappings)} intent mappings")
            except Exception as e:
                self.logger.error(f"Error loading intent mappings: {e}")

        examples_path = "original_intents.json"
        if os.path.exists(examples_path):
            try:
                with open(examples_path, 'r') as f:
                    self.original_examples = json.load(f)
                self.logger.info(f"Loaded original intent examples")
            except Exception as e:
                self.logger.error(f"Error loading original intent examples: {e}")


        self.merged_to_originals = {}
        for original, merged in self.mappings.items():
            if merged not in self.merged_to_originals:
                self.merged_to_originals[merged] = []
            self.merged_to_originals[merged].append(original)

    def identify_sub_intent(self, query, intent, confidence):
        
        result = {
            "intent": intent,
            "confidence": confidence,
            "sub_intent": None,
            "sub_confidence": 0.0
        }


        if intent not in self.merged_to_originals:
            return result


        sub_intents = self.merged_to_originals[intent]


        identified_sub_intent = self._identify_with_rules(query, intent)

        if identified_sub_intent:
            result["sub_intent"] = identified_sub_intent
            result["sub_confidence"] = 0.9
            return result


        if intent in self.original_examples:
            identified_sub_intent, confidence = self._identify_with_examples(
                query, intent, self.original_examples[intent]
            )
            if identified_sub_intent:
                result["sub_intent"] = identified_sub_intent
                result["sub_confidence"] = confidence
                return result


        if self.use_fallback:

            if intent == "database_query_comparative":
                result["sub_intent"] = "database_query_comparative_highest"
                result["sub_confidence"] = 0.5
            elif intent == "database_query_sort":
                result["sub_intent"] = "database_query_sort_ascending"
                result["sub_confidence"] = 0.5

        return result

    def _identify_with_rules(self, query, intent):
        
        query = query.lower()

        if intent == "database_query_comparative":

            highest_patterns = [
                "highest", "maximum", "most", "top", "greatest", "largest",
                "biggest", "best", "max", "highest-rated"
            ]
            for pattern in highest_patterns:
                if pattern in query:
                    return "database_query_comparative_highest"


            lowest_patterns = [
                "lowest", "minimum", "least", "bottom", "smallest",
                "cheapest", "worst", "min", "lowest-rated"
            ]
            for pattern in lowest_patterns:
                if pattern in query:
                    return "database_query_comparative_lowest"


            middle_patterns = [
                "middle", "median", "average", "mid", "center",
                "mean", "medium", "intermediate"
            ]
            for pattern in middle_patterns:
                if pattern in query:
                    return "database_query_comparative_middle"

        elif intent == "database_query_sort":

            ascending_patterns = [
                "ascending", "increasing", "low to high", "smallest to largest",
                "least to most", "lowest to highest", "asc", "up"
            ]
            for pattern in ascending_patterns:
                if pattern in query:
                    return "database_query_sort_ascending"


            descending_patterns = [
                "descending", "decreasing", "high to low", "largest to smallest",
                "most to least", "highest to lowest", "desc", "down"
            ]
            for pattern in descending_patterns:
                if pattern in query:
                    return "database_query_sort_descending"

        return None

    def _identify_with_examples(self, query, intent, examples_dict):
        

        query_words = set(query.lower().split())

        best_sub_intent = None
        best_confidence = 0.0

        for sub_intent, examples in examples_dict.items():
            max_similarity = 0.0

            for example in examples:
                example_words = set(example.lower().split())


                if query_words or example_words:
                    intersection = len(query_words.intersection(example_words))
                    union = len(query_words.union(example_words))
                    similarity = intersection / union if union > 0 else 0

                    max_similarity = max(max_similarity, similarity)

            if max_similarity > best_confidence:
                best_confidence = max_similarity
                best_sub_intent = sub_intent


        if best_confidence > 0.3:
            return best_sub_intent, best_confidence

        return None, 0.0








