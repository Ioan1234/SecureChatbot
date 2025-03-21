import json
import argparse
import logging
import random
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

from database_connector import DatabaseConnector
from model.intent_classifier import EnhancedIntentClassifier
from query_processor import QueryProcessor
from training.query_generator import DatabaseQueryGenerator
from quick_intent_merger import QuickIntentMerger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("evaluation.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def merge_test_data(test_texts, test_labels):

    merge_mappings = {
        "database_query_comparative_highest": "database_query_comparative",
        "database_query_comparative_lowest": "database_query_comparative",
        "database_query_comparative_middle": "database_query_comparative",

        "database_query_sort_ascending": "database_query_sort",
        "database_query_sort_descending": "database_query_sort",
    }

    merged_labels = []
    for label in test_labels:
        if label in merge_mappings:
            merged_labels.append(merge_mappings[label])
        else:
            merged_labels.append(label)

    return test_texts, merged_labels


def evaluate_model(model, test_texts, test_labels, use_merger=False, merge_test_data_labels=False):

    predictions = []
    confidences = []

    if merge_test_data_labels:
        test_texts, test_labels = merge_test_data(test_texts, test_labels)

    if model.model is None:
        logger.error("Model not properly initialized for evaluation")
        return {
            'accuracy': 0.0,
            'avg_confidence': 0.0,
            'classification_report': "Model not initialized",
            'confusion_matrix': [[0]]
        }

    merger = None
    if use_merger:
        merger = QuickIntentMerger(model)

    logger.info(f"Evaluating on {len(test_texts)} texts with {len(test_labels)} labels")
    logger.info(f"Available intent classes: {model.intent_classes}")
    logger.info(f"Example test texts: {test_texts[:5]}")
    logger.info(f"Using intent merging: {use_merger}")

    for i, text in enumerate(test_texts):
        try:
            if use_merger:
                result = merger.classify_intent(text)
            else:
                result = model.classify_intent(text)

            if result:
                predictions.append(result['intent'])
                confidences.append(result['confidence'])

                if 'sub_intent' in result:
                    logger.debug(f"Query: '{text}', Intent: {result['intent']}, Sub-intent: {result['sub_intent']}")
            else:
                predictions.append("unknown")
                confidences.append(0.0)

            if i % 100 == 0:
                logger.info(f"Sample prediction {i}: '{text}' -> {predictions[-1]} ({confidences[-1]:.4f})")

        except Exception as e:
            logger.error(f"Error predicting intent for text '{text}': {e}")
            predictions.append("unknown")
            confidences.append(0.0)

    unknown_count = predictions.count("unknown")
    if unknown_count > 0:
        logger.warning(f"{unknown_count} out of {len(predictions)} predictions were 'unknown'")

    correct = sum(1 for p, t in zip(predictions, test_labels) if p == t)
    accuracy = correct / len(test_labels)

    try:
        unique_labels = sorted(set(test_labels))
        report = classification_report(test_labels, predictions, labels=unique_labels)

        cm = confusion_matrix(test_labels, predictions, labels=unique_labels)

        plt.figure(figsize=(10, 8))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=unique_labels, yticklabels=unique_labels)
        plt.xlabel('Predicted')
        plt.ylabel('True')
        plt.title('Confusion Matrix')
        plt.tight_layout()

        output_file = 'confusion_matrix.png'
        if use_merger:
            output_file = 'confusion_matrix_merged.png'

        plt.savefig(output_file)

        avg_confidence = sum(confidences) / len(confidences)
    except Exception as e:
        logger.error(f"Error generating classification report: {e}")
        report = f"Error: {str(e)}"
        cm = [[0]]
        avg_confidence = 0.0

    return {
        'accuracy': accuracy,
        'avg_confidence': avg_confidence,
        'classification_report': report,
        'confusion_matrix': cm.tolist() if isinstance(cm, np.ndarray) else cm
    }


def test_query_generation(db_connector, num_samples=10):
    generator = DatabaseQueryGenerator(db_connector)

    queries, labels = generator.generate_queries()

    queries_by_intent = {}
    for query, label in zip(queries, labels):
        if label not in queries_by_intent:
            queries_by_intent[label] = []
        queries_by_intent[label].append(query)

    samples = []
    for intent, intent_queries in queries_by_intent.items():
        num_to_sample = min(num_samples, len(intent_queries))
        samples.extend(random.sample(intent_queries, num_to_sample))

    return samples


def test_query_processing(query_processor, samples, intent_classifier=None):
    results = []

    for query in samples:
        if intent_classifier:
            intent_data = intent_classifier.classify_intent(query)
        else:
            intent_data = {"intent": "database_query_list", "confidence": 0.8}

        processed_result = query_processor.process_query(query, intent_data)

        results.append({
            'query': query,
            'intent': intent_data,
            'result': processed_result
        })

    return results


def load_intent_classifier(model_path):
    logger.info(f"Loading intent classifier from {model_path}")

    if not os.path.exists(model_path):
        logger.error(f"Model path does not exist: {model_path}")
        return None

    intent_classifier = EnhancedIntentClassifier(
        vocab_size=10000,
        embedding_dim=128,
        max_sequence_length=50
    )

    success = intent_classifier.load_model(model_path)
    if not success:
        logger.error(f"Failed to load model from {model_path}")
        return None

    if intent_classifier.model is None:
        logger.error("Model object is None after loading")
        return None

    if intent_classifier.tokenizer is None:
        logger.error("Tokenizer is None after loading")
        return None

    if not intent_classifier.intent_classes:
        logger.error("No intent classes loaded")
        return None

    logger.info(f"Successfully loaded model with {len(intent_classifier.intent_classes)} intent classes")
    return intent_classifier


def main():
    parser = argparse.ArgumentParser(description="Evaluate intent classifier and query generator")
    parser.add_argument("--config", type=str, default="config.json", help="Path to configuration file")
    parser.add_argument("--model-path", type=str, default="models/intent_classifier",
                        help="Path to trained model")
    parser.add_argument("--test-data", type=str, default=None,
                        help="Path to test data (if not provided, will use generated data)")
    parser.add_argument("--num-samples", type=int, default=10,
                        help="Number of sample queries to test")
    parser.add_argument("--test-split", type=float, default=0.2,
                        help="Fraction of data to use for testing if no test data provided")
    parser.add_argument("--use-merger", action="store_true",
                        help="Use intent merging for evaluation")
    parser.add_argument("--merge-test-data", action="store_true",
                        help="Merge the test data labels")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug output")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        with open(args.config, 'r') as f:
            config = json.load(f)

        logger.info("Connecting to database...")
        db_config = config["database"]
        db_connector = DatabaseConnector(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )

        if not db_connector.connect():
            logger.error("Failed to connect to database. Exiting.")
            return False

        from encryption_manager import HomomorphicEncryptionManager
        encryption_manager = HomomorphicEncryptionManager(
            key_size=config.get("encryption", {}).get("key_size", 2048)
        )

        query_processor = QueryProcessor(
            db_connector=db_connector,
            encryption_manager=encryption_manager
        )

        logger.info(f"Loading intent classifier from {args.model_path}...")
        intent_classifier = load_intent_classifier(args.model_path)

        if intent_classifier is None:
            logger.error("Failed to load intent classifier. Exiting.")
            return False

        logger.info("Testing query generation...")
        generated_samples = test_query_generation(db_connector, args.num_samples)

        logger.info(f"Generated {len(generated_samples)} sample queries:")
        for i, sample in enumerate(generated_samples[:20]):
            logger.info(f"  {i + 1}. {sample}")

        logger.info("Testing query processing...")
        processing_results = test_query_processing(query_processor, generated_samples[:5], intent_classifier)

        logger.info("Query processing results:")
        for i, result in enumerate(processing_results):
            logger.info(f"  Query: {result['query']}")
            logger.info(f"  Result: {result['result']}")
            logger.info("")

        test_texts = []
        test_labels = []

        if args.test_data:
            logger.info(f"Loading test data from {args.test_data}...")
            with open(args.test_data, 'r') as f:
                test_data = json.load(f)
                test_texts = test_data.get("texts", [])
                test_labels = test_data.get("labels", [])
        else:
            logger.info("Using generated data for testing...")
            generator = DatabaseQueryGenerator(db_connector)
            queries, labels = generator.generate_queries()

            conversational_data = {
                "greeting": [
                    "Hello", "Hi there", "Good morning", "Hey", "Greetings",
                    "Hi chatbot", "Hello there", "Morning", "Good afternoon"
                ],
                "help": [
                    "Help", "I need help", "What can you do", "How do I use this",
                    "Show me what you can do", "Instructions", "Guide me"
                ],
                "goodbye": [
                    "Goodbye", "Bye", "Exit", "Quit", "End", "See you later",
                    "I'm done", "Close", "That's all", "Thanks, bye"
                ]
            }

            for intent, intent_queries in conversational_data.items():
                for query in intent_queries:
                    queries.append(query)
                    labels.append(intent)

            indices = np.arange(len(queries))
            np.random.shuffle(indices)
            split_idx = int(len(queries) * args.test_split)
            test_indices = indices[:split_idx]

            test_texts = [queries[i] for i in test_indices]
            test_labels = [labels[i] for i in test_indices]

        if test_texts and test_labels:
            logger.info(f"Evaluating model on {len(test_texts)} test examples...")

            standard_results = evaluate_model(
                intent_classifier,
                test_texts,
                test_labels,
                use_merger=False,
                merge_test_data_labels=False
            )

            logger.info("=== Standard Evaluation Results ===")
            logger.info(f"Accuracy: {standard_results['accuracy']:.4f}")
            logger.info(f"Average confidence: {standard_results['avg_confidence']:.4f}")
            logger.info("Classification report:")
            logger.info(standard_results['classification_report'])

            with open("standard_evaluation_results.json", 'w') as f:
                json.dump({
                    'accuracy': standard_results['accuracy'],
                    'avg_confidence': standard_results['avg_confidence'],
                    'confusion_matrix': standard_results['confusion_matrix']
                }, f, indent=2)

            if args.use_merger:
                logger.info("Running evaluation with intent merging...")
                merger_results = evaluate_model(
                    intent_classifier,
                    test_texts,
                    test_labels,
                    use_merger=True,
                    merge_test_data_labels=args.merge_test_data
                )

                logger.info("=== Merged Intent Evaluation Results ===")
                logger.info(f"Accuracy: {merger_results['accuracy']:.4f}")
                logger.info(f"Average confidence: {merger_results['avg_confidence']:.4f}")
                logger.info("Classification report:")
                logger.info(merger_results['classification_report'])

                with open("merged_evaluation_results.json", 'w') as f:
                    json.dump({
                        'accuracy': merger_results['accuracy'],
                        'avg_confidence': merger_results['avg_confidence'],
                        'confusion_matrix': merger_results['confusion_matrix']
                    }, f, indent=2)

                logger.info("Confusion matrix saved to confusion_matrix_merged.png")
                logger.info("Merged evaluation results saved to merged_evaluation_results.json")

            logger.info("Standard confusion matrix saved to confusion_matrix.png")
            logger.info("Standard evaluation results saved to standard_evaluation_results.json")
        else:
            logger.warning("No test data available for model evaluation")

        return True
    except Exception as e:
        logger.error(f"Error in evaluation process: {e}", exc_info=True)
        return False
    finally:
        if 'db_connector' in locals() and db_connector:
            db_connector.disconnect()


if __name__ == "__main__":
    logger.info("=== Starting model evaluation ===")
    success = main()
    logger.info(f"=== Evaluation {'completed successfully' if success else 'failed'} ===")