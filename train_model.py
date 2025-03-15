import sys
import traceback
import os
import json
import logging
import argparse
import numpy as np
import tensorflow as tf
from datetime import datetime
from sklearn.model_selection import KFold
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau

from database_connector import DatabaseConnector
from training.query_generator import DatabaseQueryGenerator
from model.intent_classifier import IntentClassifier

print("Starting train_model.py")
sys.stdout.flush()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("training.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def augment_text_data_simple(texts, labels, augmentation_factor=0.3):
    from random import choice, random, shuffle, randint

    augmented_texts = texts.copy()
    augmented_labels = labels.copy()

    num_to_augment = int(len(texts) * augmentation_factor)
    indices_to_augment = list(range(len(texts)))
    shuffle(indices_to_augment)
    indices_to_augment = indices_to_augment[:num_to_augment]

    logger.info(f"Augmenting {num_to_augment} examples with simple methods")

    replacements = {
        "show": ["display", "list", "get", "find", "retrieve"],
        "list": ["show", "display", "get", "enumerate"],
        "find": ["locate", "get", "search for", "show"],
        "display": ["show", "present", "list", "get"],
        "get": ["retrieve", "obtain", "fetch", "show"],
        "all": ["every", "each", "the complete set of", "the full list of"],
        "where": ["with", "having", "that have", "with the condition"],
        "equal to": ["is", "=", "matching", "that is"],
        "sorted by": ["ordered by", "arranged by", "in order of"],
        "recent": ["latest", "newest", "current", "fresh"],
        "markets": ["exchanges", "trading venues", "market places"],
        "traders": ["users", "trading accounts", "people trading"],
        "trades": ["transactions", "exchanges", "deals"],
        "brokers": ["agents", "intermediaries", "broker firms"],
        "assets": ["securities", "instruments", "holdings", "investments"],
        "highest": ["maximum", "top", "greatest", "largest", "biggest"],
        "lowest": ["minimum", "bottom", "smallest", "least"],
        "middle": ["median", "average", "mid-range", "center"],
        "ascending": ["increasing", "low to high", "smallest to largest"],
        "descending": ["decreasing", "high to low", "largest to smallest"]
    }

    for idx in indices_to_augment:
        text = texts[idx]
        label = labels[idx]

        transform_method = choice(["replace_word", "word_order", "word_removal"])

        if transform_method == "replace_word":
            for original, alternatives in replacements.items():
                if original in text.lower():
                    alternative = choice(alternatives)
                    if text.find(original) >= 0:
                        augmented_text = text.replace(original, alternative)
                    elif text.find(original.capitalize()) >= 0:
                        augmented_text = text.replace(original.capitalize(), alternative.capitalize())
                    else:
                        augmented_text = text.lower().replace(original, alternative)
                    break
            else:
                continue

        elif transform_method == "word_order" and " " in text:
            words = text.split()
            if len(words) > 3:
                idx1 = randint(1, len(words) - 2)
                words[idx1], words[idx1 + 1] = words[idx1 + 1], words[idx1]
                augmented_text = " ".join(words)
            else:
                continue

        elif transform_method == "word_removal" and " " in text:
            words = text.split()
            if len(words) > 4:
                skip_words = ["a", "the", "and", "or", "for", "with", "me", "to"]
                remove_candidates = []
                for i, word in enumerate(words):
                    if word.lower() in skip_words:
                        remove_candidates.append(i)

                if remove_candidates:
                    remove_idx = choice(remove_candidates)
                    words.pop(remove_idx)
                    augmented_text = " ".join(words)
                else:
                    continue
            else:
                continue
        else:
            continue

        augmented_texts.append(augmented_text)
        augmented_labels.append(label)

    logger.info(f"Data augmentation complete. Original size: {len(texts)}, New size: {len(augmented_texts)}")
    unique_texts = []
    unique_labels = []
    seen = set()

    for text, label in zip(augmented_texts, augmented_labels):
        if text.lower() not in seen:
            seen.add(text.lower())
            unique_texts.append(text)
            unique_labels.append(label)

    logger.info(f"Comparative augmentation complete. Original size: {len(texts)}, New size: {len(unique_texts)}")
    return unique_texts, unique_labels


def augment_comparative_queries(texts, labels):
    from random import choice, shuffle

    augmented_texts = texts.copy()
    augmented_labels = labels.copy()

    comparative_types = ["comparative_highest", "comparative_lowest", "comparative_middle",
                         "sort_ascending", "sort_descending"]

    comparative_indices = [
        i for i, label in enumerate(labels)
        if any(comp_type in label for comp_type in comparative_types)
    ]

    shuffle(comparative_indices)

    comparative_indices = comparative_indices[:int(len(comparative_indices) * 0.3)]

    logger.info(f"Augmenting {len(comparative_indices)} comparative queries")

    superlative_replacements = {
        "highest": ["maximum", "greatest", "largest", "top", "best"],
        "lowest": ["minimum", "smallest", "least", "bottom", "worst"],
        "middle": ["median", "average", "mid-range", "center"],
        "ascending": ["increasing", "growing", "rising", "upward"],
        "descending": ["decreasing", "falling", "downward", "reducing"]
    }

    entity_replacements = {
        "price": ["cost", "value", "worth", "rate"],
        "value": ["amount", "total", "sum", "worth"],
        "trades": ["transactions", "deals", "exchanges"],
        "assets": ["stocks", "securities", "instruments", "investments"],
        "traders": ["users", "clients", "customers", "accounts"]
    }

    for idx in comparative_indices:
        text = texts[idx]
        label = labels[idx]

        replace_type = choice(["superlative", "entity", "attribute"])

        augmented = False
        if replace_type == "superlative":
            for term, alternatives in superlative_replacements.items():
                if term in text.lower():
                    alternative = choice(alternatives)
                    augmented_text = text.replace(term, alternative)
                    if term.capitalize() in text:
                        augmented_text = text.replace(term.capitalize(), alternative.capitalize())
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break

        elif replace_type == "entity" and not augmented:
            for entity, alternatives in entity_replacements.items():
                if entity in text.lower():
                    alternative = choice(alternatives)
                    augmented_text = text.replace(entity, alternative)
                    if entity.capitalize() in text:
                        augmented_text = text.replace(entity.capitalize(), alternative.capitalize())
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break

        elif replace_type == "attribute" and not augmented:
            words = text.split()
            for i, word in enumerate(words):
                if word.lower() in entity_replacements:
                    alternatives = entity_replacements[word.lower()]
                    alternative = choice(alternatives)
                    if word[0].isupper():
                        alternative = alternative.capitalize()
                    words[i] = alternative
                    augmented_text = " ".join(words)
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break

    return augmented_texts, augmented_labels

def prepare_training_data(args):
    try:
        logger.info("Loading database configuration...")

        config_path = args.config
        logger.info(f"Using config file: {config_path}")

        with open(config_path, 'r') as f:
            config = json.load(f)

        db_config = config["database"]
        logger.info(f"Connecting to database {db_config['database']} at {db_config['host']}...")

        db_connector = DatabaseConnector(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )

        connected = db_connector.connect()
        logger.info(f"Database connection result: {connected}")

        if not connected:
            logger.error("Failed to connect to database")
            return False

        try:
            output_path = args.output
            logger.info(f"Setting up query generator with output path: {output_path}")

            generator = DatabaseQueryGenerator(db_connector, output_path)

            training_data = {}

            if args.enrich:
                logger.info("Enriching existing training data")
                training_data = generator.enrich_existing_training_data()
                logger.info(f"Enrichment complete, got {len(training_data.get('texts', []))} examples")
            else:
                logger.info("Generating new training data")
                training_data = generator.generate_training_data()
                logger.info(f"Generation complete, got {len(training_data.get('texts', []))} examples")

            if not training_data:
                logger.error("No training data returned")
                return False

            texts = training_data.get("texts")
            labels = training_data.get("labels")

            if not texts or not labels:
                logger.error(f"Training data missing required keys. Got keys: {training_data.keys()}")
                return False

            if len(texts) == 0 or len(labels) == 0:
                logger.error("Training data contains empty lists")
                return False

            if len(texts) != len(labels):
                logger.error(f"Mismatch between texts ({len(texts)}) and labels ({len(labels)})")
                return False

            logger.info(f"Successfully prepared {len(texts)} training examples")

            if args.generate_only:
                logger.info("Generate-only mode. Skipping model training.")
                return True

            return training_data
        finally:
            db_connector.disconnect()
            logger.info("Database connection closed")

    except Exception as e:
        logger.error(f"Error preparing training data: {e}")
        traceback.print_exc()
        return False


def train_model_with_data(training_data, args):

    try:
        logger.info("Starting model training...")

        texts = training_data["texts"]
        labels = training_data["labels"]

        logger.info(f"Training with {len(texts)} examples across {len(set(labels))} intent classes")

        if args.augment:
            logger.info("Applying data augmentation...")
            texts, labels = augment_text_data_simple(texts, labels)
            logger.info(f"After basic augmentation: {len(texts)} examples")

            if any("comparative" in label for label in labels):
                logger.info("Applying specialized comparative query augmentation...")
                texts, labels = augment_comparative_queries(texts, labels)
                logger.info(f"After comparative augmentation: {len(texts)} examples")

        logger.info("Tokenizing text data...")
        tokenizer = Tokenizer(num_words=5000)
        tokenizer.fit_on_texts(texts)

        sequences = tokenizer.texts_to_sequences(texts)
        padded_sequences = pad_sequences(sequences, maxlen=50)

        unique_labels = sorted(set(labels))
        logger.info(f"Intent classes: {unique_labels}")

        label_to_index = {label: i for i, label in enumerate(unique_labels)}
        label_indices = [label_to_index[label] for label in labels]
        one_hot_labels = to_categorical(label_indices)

        model_path = args.model_output
        logger.info(f"Creating intent classifier with output to {model_path}")

        classifier = IntentClassifier()

        if args.reduce_complexity:
            logger.info("Using simplified model architecture")
            model = classifier.build_model(len(unique_labels))
        else:
            logger.info("Using regularized model architecture")
            model = classifier.build_regularized_model(len(unique_labels))

        callbacks = []

        if args.early_stopping:
            logger.info("Adding early stopping callback")
            early_stopping = EarlyStopping(
                monitor='val_loss',
                patience=5,
                restore_best_weights=True
            )
            callbacks.append(early_stopping)

        os.makedirs(model_path, exist_ok=True)

        checkpoint_path = os.path.join(model_path, "checkpoint.h5")
        checkpoint = ModelCheckpoint(
            checkpoint_path,
            monitor='val_loss',
            save_best_only=True
        )
        callbacks.append(checkpoint)

        reduce_lr = ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.2,
            patience=3,
            min_lr=0.001
        )
        callbacks.append(reduce_lr)

        logger.info("Beginning model training...")

        model.fit(
            padded_sequences,
            one_hot_labels,
            epochs=20,
            batch_size=32,
            validation_split=0.2,
            callbacks=callbacks,
            verbose=1
        )

        logger.info(f"Training complete. Saving model to {model_path}")
        classifier.model = model
        classifier.tokenizer = tokenizer
        classifier.intent_classes = unique_labels

        save_result = classifier.save_model(model_path)
        if save_result:
            logger.info("Model saved successfully")
        else:
            logger.error("Failed to save model")
            return False

        return True

    except Exception as e:
        logger.error(f"Error in model training: {e}")
        traceback.print_exc()
        return False


def main():
    logger.info("Parsing command line arguments...")

    parser = argparse.ArgumentParser(description="Train intent classifier model")

    parser.add_argument("--config", type=str, default="config.json", help="Path to configuration file")
    parser.add_argument("--generate-only", action="store_true", help="Only generate training data without training")
    parser.add_argument("--enrich", action="store_true", help="Enrich existing training data instead of replacing it")
    parser.add_argument("--augment", action="store_true", help="Apply data augmentation techniques")
    parser.add_argument("--output", type=str, default="training/generated_training_data.json",
                        help="Output path for generated training data")
    parser.add_argument("--model-output", type=str, default="models/intent_classifier",
                        help="Output directory for trained model")
    parser.add_argument("--early-stopping", action="store_true", help="Enable early stopping based on validation loss")
    parser.add_argument("--reduce-complexity", action="store_true", help="Use a simpler model architecture")
    parser.add_argument("--cross-validation", type=int, default=0,
                        help="Number of cross-validation folds (0 to disable)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()
    logger.info(f"Arguments: {args}")

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug mode enabled")

    logger.info("Preparing training data...")
    training_data = prepare_training_data(args)

    if training_data is True:
        logger.info("Training data generation complete. Exiting as requested.")
        return True
    elif not training_data:
        logger.error("Failed to prepare training data. Exiting.")
        return False

    logger.info("Proceeding to model training...")
    training_success = train_model_with_data(training_data, args)

    if training_success:
        logger.info("Model training completed successfully")
        return True
    else:
        logger.error("Model training failed")
        return False


if __name__ == "__main__":
    logger.info("=== Starting intent classifier training ===")
    success = main()
    logger.info(f"=== Training {'completed successfully' if success else 'failed'} ===")
    sys.exit(0 if success else 1)