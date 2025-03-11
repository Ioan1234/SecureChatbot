
import json
import argparse
import logging
import random
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

from database_connector import DatabaseConnector
from model.intent_classifier import IntentClassifier
from query_processor import QueryProcessor
from training.query_generator import DatabaseQueryGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("evaluation.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def evaluate_model(model, test_texts, test_labels):
    predictions = []
    confidences = []

    for text in test_texts:
        result = model.classify_intent(text)
        if result:
            predictions.append(result['intent'])
            confidences.append(result['confidence'])
        else:
            predictions.append("unknown")
            confidences.append(0.0)

    correct = sum(1 for p, t in zip(predictions, test_labels) if p == t)
    accuracy = correct / len(test_labels)

    unique_labels = sorted(set(test_labels))
    report = classification_report(test_labels, predictions, labels=unique_labels)

    cm = confusion_matrix(test_labels, predictions, labels=unique_labels)

    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=unique_labels, yticklabels=unique_labels)
    plt.xlabel('Predicted')
    plt.ylabel('True')
    plt.title('Confusion Matrix')
    plt.tight_layout()
    plt.savefig('confusion_matrix.png')

    avg_confidence = sum(confidences) / len(confidences)

    return {
        'accuracy': accuracy,
        'avg_confidence': avg_confidence,
        'classification_report': report,
        'confusion_matrix': cm.tolist()
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


def test_query_processing(query_processor, samples):
    results = []

    for query in samples:
        sql = query_processor.natural_language_to_sql(query)

        results.append({
            'query': query,
            'sql': sql
        })

    return results


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
    args = parser.parse_args()

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
        intent_classifier = IntentClassifier(model_path=args.model_path)

        logger.info("Testing query generation...")
        generated_samples = test_query_generation(db_connector, args.num_samples)

        logger.info(f"Generated {len(generated_samples)} sample queries:")
        for i, sample in enumerate(generated_samples[:20]):  # Show first 20 samples
            logger.info(f"  {i + 1}. {sample}")

        logger.info("Testing query processing...")
        processing_results = test_query_processing(query_processor, generated_samples[:5])

        logger.info("Query processing results:")
        for i, result in enumerate(processing_results):
            logger.info(f"  Query: {result['query']}")
            logger.info(f"  SQL  : {result['sql']}")
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
            evaluation_results = evaluate_model(intent_classifier, test_texts, test_labels)

            logger.info(f"Accuracy: {evaluation_results['accuracy']:.4f}")
            logger.info(f"Average confidence: {evaluation_results['avg_confidence']:.4f}")
            logger.info("Classification report:")
            logger.info(evaluation_results['classification_report'])

            with open("evaluation_results.json", 'w') as f:
                json.dump({
                    'accuracy': evaluation_results['accuracy'],
                    'avg_confidence': evaluation_results['avg_confidence'],
                    'confusion_matrix': evaluation_results['confusion_matrix']
                }, f, indent=2)

            logger.info("Confusion matrix saved to confusion_matrix.png")
            logger.info("Evaluation results saved to evaluation_results.json")
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