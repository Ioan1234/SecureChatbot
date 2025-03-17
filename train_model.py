import sys
import traceback
import os
import json
import logging
import argparse
import random as rnd
from random import choice, shuffle

import tensorflow as tf
from datetime import datetime
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau

from database_connector import DatabaseConnector
from training.query_generator import DatabaseQueryGenerator
from model.intent_classifier import EnhancedIntentClassifier

print("Starting enhanced training script with intent merging")
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

MERGE_MAPPINGS = {
    "database_query_comparative_highest": "database_query_comparative",
    "database_query_comparative_lowest": "database_query_comparative",
    "database_query_comparative_middle": "database_query_comparative",

    "database_query_sort_ascending": "database_query_sort",
    "database_query_sort_descending": "database_query_sort",
}

ORIGINAL_INTENTS = {}


def merge_intent_classes(texts, labels):
    global ORIGINAL_INTENTS

    ORIGINAL_INTENTS = {}

    merged_labels = []
    for i, label in enumerate(labels):
        if label in MERGE_MAPPINGS:
            merged_label = MERGE_MAPPINGS[label]
            if merged_label not in ORIGINAL_INTENTS:
                ORIGINAL_INTENTS[merged_label] = {}

            if label not in ORIGINAL_INTENTS[merged_label]:
                ORIGINAL_INTENTS[merged_label][label] = []
            ORIGINAL_INTENTS[merged_label][label].append(texts[i])

            merged_labels.append(merged_label)
        else:
            merged_labels.append(label)

    with open("original_intents.json", "w") as f:
        json.dump(ORIGINAL_INTENTS, f, indent=2)

    original_classes = set(labels)
    merged_classes = set(merged_labels)

    logger.info(f"Merged {len(original_classes)} intent classes into {len(merged_classes)} classes")
    logger.info(f"Original classes: {sorted(list(original_classes))}")
    logger.info(f"Merged classes: {sorted(list(merged_classes))}")

    merged_counts = {}
    for label in merged_labels:
        if label not in merged_counts:
            merged_counts[label] = 0
        merged_counts[label] += 1

    logger.info(f"Merged class counts: {merged_counts}")

    return texts, merged_labels


def create_additional_examples(merged_class, original_examples):
    additional_texts = []
    additional_labels = []

    if merged_class == "database_query_comparative":
        templates = [
            "Show me the {position} {entity} by {attribute}",
            "Find the {position} {attribute} {entity}",
            "Which {entity} {verb} the {position} {attribute}",
            "Sort {entity} and show me the {position} ones",
            "List {entity} with {position} {attribute} values",
            "{position} {attribute} {entity} please",
            "What {entity} has the {position} {attribute}",
            "Give me {entity} ranked by {position} {attribute}",
        ]

        entities = ["assets", "trades", "traders", "markets", "accounts", "stocks", "prices"]
        attributes = ["price", "value", "cost", "amount", "balance", "volume", "quantity", "date"]

        position_highest = ["highest", "greatest", "maximum", "largest", "biggest", "top"]
        verbs_highest = ["have", "has", "showing", "with", "containing"]

        for template in templates:
            for entity in entities:
                for attribute in attributes:
                    for position in position_highest:
                        for _ in range(2):
                            verb = choice(verbs_highest)
                            text = template.format(
                                position=position,
                                entity=entity,
                                attribute=attribute,
                                verb=verb
                            )
                            additional_texts.append(text)
                            additional_labels.append("database_query_comparative")

        position_lowest = ["lowest", "least", "minimum", "smallest", "bottom"]
        verbs_lowest = ["have", "has", "showing", "with", "containing"]

        for template in templates:
            for entity in entities:
                for attribute in attributes:
                    for position in position_lowest:
                        for _ in range(2):
                            verb = choice(verbs_lowest)
                            text = template.format(
                                position=position,
                                entity=entity,
                                attribute=attribute,
                                verb=verb
                            )
                            additional_texts.append(text)
                            additional_labels.append("database_query_comparative")

    elif merged_class == "database_query_sort":
        templates = [
            "Sort {entity} by {attribute} in {direction} order",
            "Show {entity} sorted by {attribute} {direction}",
            "List {entity} {direction} by {attribute}",
            "Order {entity} by {attribute} {direction}",
            "Display {entity} in {direction} order of {attribute}",
            "{entity} ordered by {attribute} {direction}",
            "Arrange {entity} by {attribute} from {from_to}",
            "Show me {entity} with {attribute} arranged {direction}",
        ]

        entities = ["assets", "trades", "traders", "markets", "accounts", "stocks", "prices"]
        attributes = ["price", "value", "cost", "amount", "balance", "volume", "quantity", "date"]

        direction_asc = ["ascending", "increasing", "rising", "upward"]
        from_to_asc = ["low to high", "smallest to largest", "least to most"]

        for template in templates:
            for entity in entities:
                for attribute in attributes:
                    for direction in direction_asc:
                        for _ in range(2):
                            from_to = choice(from_to_asc) if "{from_to}" in template else ""
                            text = template.format(
                                entity=entity,
                                attribute=attribute,
                                direction=direction,
                                from_to=from_to
                            )
                            additional_texts.append(text)
                            additional_labels.append("database_query_sort")

        direction_desc = ["descending", "decreasing", "falling", "downward"]
        from_to_desc = ["high to low", "largest to smallest", "most to least"]

        for template in templates:
            for entity in entities:
                for attribute in attributes:
                    for direction in direction_desc:
                        for _ in range(2):
                            from_to = choice(from_to_desc) if "{from_to}" in template else ""
                            text = template.format(
                                entity=entity,
                                attribute=attribute,
                                direction=direction,
                                from_to=from_to
                            )
                            additional_texts.append(text)
                            additional_labels.append("database_query_sort")

    unique_examples = {}
    for text, label in zip(additional_texts, additional_labels):
        unique_examples[text.lower()] = label

    shuffled_texts = list(unique_examples.keys())
    shuffle(shuffled_texts)

    logger.info(f"Created {len(shuffled_texts)} additional examples for {merged_class}")

    return shuffled_texts, [unique_examples[text.lower()] for text in shuffled_texts]


def augment_text_data(texts, labels, augmentation_factor=0.3):

    augmented_texts = texts.copy()
    augmented_labels = labels.copy()

    texts_by_label = {}
    for text, label in zip(texts, labels):
        if label not in texts_by_label:
            texts_by_label[label] = []
        texts_by_label[label].append(text)

    label_counts = {label: len(examples) for label, examples in texts_by_label.items()}
    logger.info(f"Original class distribution: {label_counts}")

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
        "traders": ["users", "trading accounts", "people trading", "clients"],
        "trades": ["transactions", "exchanges", "deals", "trading activity"],
        "brokers": ["agents", "intermediaries", "broker firms"],
        "assets": ["securities", "instruments", "holdings", "investments"],
        "highest": ["maximum", "top", "greatest", "largest", "biggest"],
        "lowest": ["minimum", "bottom", "smallest", "least"],
        "middle": ["median", "average", "mid-range", "center"],
        "ascending": ["increasing", "low to high", "smallest to largest"],
        "descending": ["decreasing", "high to low", "largest to smallest"]
    }

    problem_classes = ["database_query_comparative", "database_query_sort"]

    for label in problem_classes:
        if label in texts_by_label:
            source_texts = texts_by_label[label]
            logger.info(f"Applying enhanced augmentation for {label} with {len(source_texts)} examples")

            for text in source_texts:
                for _ in range(3):
                    augmented = False

                    for original, alternatives in replacements.items():
                        if original in text.lower():
                            alternative = choice(alternatives)
                            augmented_text = text.replace(original, alternative)
                            if augmented_text != text:
                                augmented_texts.append(augmented_text)
                                augmented_labels.append(label)
                                augmented = True
                                break

                    if not augmented and " " in text:
                        words = text.split()
                        if len(words) > 4:
                            idx1 = rnd.randint(1, len(words) - 2)
                            words[idx1], words[idx1 + 1] = words[idx1 + 1], words[idx1]
                            augmented_text = " ".join(words)
                            augmented_texts.append(augmented_text)
                            augmented_labels.append(label)

    if augmentation_factor > 0:
        num_to_augment = int(len(texts) * augmentation_factor)
        indices_to_augment = list(range(len(texts)))
        shuffle(indices_to_augment)
        indices_to_augment = indices_to_augment[:num_to_augment]

        for idx in indices_to_augment:
            text = texts[idx]
            label = labels[idx]

            for original, alternatives in replacements.items():
                if original in text.lower():
                    alternative = choice(alternatives)
                    augmented_text = text.replace(original, alternative)
                    if augmented_text != text:
                        augmented_texts.append(augmented_text)
                        augmented_labels.append(label)
                        break

    unique_texts = []
    unique_labels = []
    seen = set()

    for text, label in zip(augmented_texts, augmented_labels):
        if text.lower() not in seen:
            seen.add(text.lower())
            unique_texts.append(text)
            unique_labels.append(label)

    for problem_class in problem_classes:
        original_examples = []
        if problem_class in texts_by_label:
            original_examples = texts_by_label[problem_class]

        additional_texts, additional_labels = create_additional_examples(
            problem_class, original_examples
        )

        unique_texts.extend(additional_texts)
        unique_labels.extend(additional_labels)

    final_counts = {}
    for label in unique_labels:
        if label not in final_counts:
            final_counts[label] = 0
        final_counts[label] += 1

    logger.info(f"After augmentation: {len(unique_texts)} examples")
    logger.info(f"Final class distribution: {final_counts}")

    return unique_texts, unique_labels


def balance_classes(texts, labels, min_per_class=40, max_per_class=800):
    classes = {}
    for text, label in zip(texts, labels):
        if label not in classes:
            classes[label] = []
        classes[label].append(text)

    balanced_texts = []
    balanced_labels = []

    problem_classes = ["database_query_comparative", "database_query_sort"]
    normal_max = max_per_class
    problem_max = max_per_class * 2

    for label, examples in classes.items():
        if len(examples) < min_per_class:
            multiplier = min_per_class // len(examples) + 1
            examples_to_use = examples * multiplier
            logger.info(f"Class {label}: Duplicated from {len(examples)} to {len(examples_to_use)} examples")
        else:
            examples_to_use = examples

        this_max = problem_max if label in problem_classes else normal_max

        examples_to_use = examples_to_use[:this_max]

        balanced_texts.extend(examples_to_use)
        balanced_labels.extend([label] * len(examples_to_use))

    combined = list(zip(balanced_texts, balanced_labels))
    shuffle(combined)
    balanced_texts, balanced_labels = zip(*combined)

    balanced_texts = list(balanced_texts)
    balanced_labels = list(balanced_labels)

    logger.info(f"After balancing: {len(balanced_texts)} total examples")

    final_counts = {}
    for label in balanced_labels:
        final_counts[label] = final_counts.get(label, 0) + 1

    logger.info(f"Final class distribution: {final_counts}")

    return balanced_texts, balanced_labels


def build_multi_filter_model(vocab_size, embedding_dim, max_sequence_length, num_classes):

    inputs = tf.keras.layers.Input(shape=(max_sequence_length,))
    embedding = tf.keras.layers.Embedding(
        vocab_size, embedding_dim, input_length=max_sequence_length
    )(inputs)

    conv3 = tf.keras.layers.Conv1D(128, 3, padding='same', activation='relu')(embedding)
    conv4 = tf.keras.layers.Conv1D(128, 4, padding='same', activation='relu')(embedding)
    conv5 = tf.keras.layers.Conv1D(128, 5, padding='same', activation='relu')(embedding)

    pool3 = tf.keras.layers.GlobalMaxPooling1D()(conv3)
    pool4 = tf.keras.layers.GlobalMaxPooling1D()(conv4)
    pool5 = tf.keras.layers.GlobalMaxPooling1D()(conv5)

    concat = tf.keras.layers.Concatenate()([pool3, pool4, pool5])

    dropout = tf.keras.layers.Dropout(0.5)(concat)

    dense1 = tf.keras.layers.Dense(256, activation='relu')(dropout)
    dropout2 = tf.keras.layers.Dropout(0.5)(dense1)

    outputs = tf.keras.layers.Dense(num_classes, activation='softmax')(dropout2)

    model = tf.keras.Model(inputs=inputs, outputs=outputs)

    model.compile(
        loss='categorical_crossentropy',
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
        metrics=['accuracy']
    )

    return model


def prepare_training_data(args):
    try:
        if args.use_existing_data and os.path.exists(args.output):
            try:
                logger.info(f"Loading existing training data from {args.output}")
                with open(args.output, 'r') as f:
                    training_data = json.load(f)

                texts = training_data.get("texts", [])
                labels = training_data.get("labels", [])

                if texts and labels and len(texts) == len(labels):
                    logger.info(f"Loaded {len(texts)} examples from existing file")
                    return {"texts": texts, "labels": labels}
                else:
                    logger.warning("Existing data file is invalid. Will generate new data.")
            except Exception as e:
                logger.warning(f"Error loading existing data: {e}")

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
        logger.info("Starting model training process...")

        texts = training_data["texts"]
        labels = training_data["labels"]

        if args.merge_classes:
            texts, labels = merge_intent_classes(texts, labels)

        balanced_texts, balanced_labels = balance_classes(
            texts,
            labels,
            min_per_class=args.min_per_class,
            max_per_class=args.max_per_class
        )
        texts, labels = balanced_texts, balanced_labels

        if args.augment:
            texts, labels = augment_text_data(
                texts,
                labels,
                augmentation_factor=args.augmentation_factor
            )

        logger.info("Tokenizing and preparing sequences...")
        tokenizer = Tokenizer(num_words=args.vocab_size)
        tokenizer.fit_on_texts(texts)

        sequences = tokenizer.texts_to_sequences(texts)
        padded_sequences = pad_sequences(sequences, maxlen=args.max_sequence_length)

        unique_labels = sorted(set(labels))
        logger.info(f"Training with {len(unique_labels)} intent classes: {unique_labels}")

        label_to_index = {label: i for i, label in enumerate(unique_labels)}
        label_indices = [label_to_index[label] for label in labels]
        one_hot_labels = to_categorical(label_indices)

        model_path = args.model_output
        logger.info(f"Will save model to {model_path}")
        os.makedirs(model_path, exist_ok=True)

        classifier = EnhancedIntentClassifier(
            vocab_size=args.vocab_size,
            embedding_dim=args.embedding_dim,
            max_sequence_length=args.max_sequence_length
        )

        model = build_multi_filter_model(
            args.vocab_size,
            args.embedding_dim,
            args.max_sequence_length,
            len(unique_labels)
        )

        logger.info("Model architecture:")
        model.summary(print_fn=logger.info)

        callbacks = []

        if args.early_stopping:
            early_stopping = EarlyStopping(
                monitor='val_accuracy',
                patience=args.patience,
                restore_best_weights=True
            )
            callbacks.append(early_stopping)
            logger.info(f"Added early stopping with patience={args.patience}")

        checkpoint_path = os.path.join(model_path, "checkpoint.h5")
        checkpoint = ModelCheckpoint(
            checkpoint_path,
            monitor='val_accuracy',
            save_best_only=True
        )
        callbacks.append(checkpoint)

        reduce_lr = ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.2,
            patience=args.patience // 2,
            min_lr=0.0001
        )
        callbacks.append(reduce_lr)

        logger.info(f"Starting training with {args.epochs} epochs...")
        history = model.fit(
            padded_sequences,
            one_hot_labels,
            epochs=args.epochs,
            batch_size=args.batch_size,
            validation_split=args.validation_split,
            callbacks=callbacks,
            verbose=1
        )

        val_acc = history.history['val_accuracy']
        logger.info(f"Validation accuracy progression: {val_acc}")
        logger.info(f"Best validation accuracy: {max(val_acc)}")

        logger.info(f"Training complete. Saving model to {model_path}")
        classifier.model = model
        classifier.tokenizer = tokenizer
        classifier.intent_classes = unique_labels

        save_result = classifier.save_model(model_path)
        if save_result:
            logger.info("Model saved successfully")

            model_h5_path = os.path.join(model_path, "model.h5")
            model.save(model_h5_path)
            logger.info(f"Additional model file saved to {model_h5_path}")

            mappings_path = os.path.join(model_path, "intent_mappings.json")
            with open(mappings_path, 'w') as f:
                json.dump({
                    "merge_mappings": MERGE_MAPPINGS,
                }, f, indent=2)
            logger.info(f"Intent mappings saved to {mappings_path}")

            return True
        else:
            logger.error("Failed to save model")
            return False

    except Exception as e:
        logger.error(f"Error in model training: {e}")
        traceback.print_exc()
        return False


def main():
    logger.info("Parsing command line arguments...")

    parser = argparse.ArgumentParser(description="Train intent classifier with merged classes")

    parser.add_argument("--config", type=str, default="config.json", help="Path to configuration file")
    parser.add_argument("--generate-only", action="store_true", help="Only generate training data without training")
    parser.add_argument("--enrich", action="store_true", help="Enrich existing training data instead of replacing it")
    parser.add_argument("--augment", action="store_true", help="Apply data augmentation techniques")
    parser.add_argument("--output", type=str, default="training/generated_training_data.json",
                        help="Output path for generated training data")
    parser.add_argument("--model-output", type=str, default="models/intent_classifier_merged",
                        help="Output directory for trained model")
    parser.add_argument("--early-stopping", action="store_true",
                        help="Enable early stopping based on validation accuracy")
    parser.add_argument("--epochs", type=int, default=30, help="Number of training epochs")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size for training")
    parser.add_argument("--vocab-size", type=int, default=10000, help="Vocabulary size for tokenizer")
    parser.add_argument("--embedding-dim", type=int, default=150, help="Embedding dimension")
    parser.add_argument("--max-sequence-length", type=int, default=50, help="Maximum sequence length")
    parser.add_argument("--patience", type=int, default=8, help="Patience for early stopping")
    parser.add_argument("--validation-split", type=float, default=0.2, help="Validation split ratio")
    parser.add_argument("--min-per-class", type=int, default=50, help="Minimum samples per class")
    parser.add_argument("--max-per-class", type=int, default=800, help="Maximum samples per class")
    parser.add_argument("--merge-classes", action="store_true", help="Merge similar intent classes")
    parser.add_argument("--augmentation-factor", type=float, default=0.3, help="Data augmentation factor")
    parser.add_argument("--use-existing-data", action="store_true", help="Use existing training data file if available")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()
    logger.info(f"Arguments: {args}")

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug mode enabled")

    if not args.generate_only and os.path.exists(args.model_output):
        logger.info(f"Removing existing model files from {args.model_output}")
        for file in os.listdir(args.model_output):
            file_path = os.path.join(args.model_output, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

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
    logger.info("=== Starting intent merger training ===")
    success = main()
    logger.info(f"=== Training {'completed successfully' if success else 'failed'} ===")
    sys.exit(0 if success else 1)