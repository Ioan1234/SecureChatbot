
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
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau

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
        "assets": ["securities", "instruments", "holdings", "investments"]
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
    return augmented_texts, augmented_labels


def main():
    parser = argparse.ArgumentParser(description="Enhanced model training with regularization and early stopping")
    parser.add_argument("--config", type=str, default="config.json", help="Path to configuration file")
    parser.add_argument("--generate-only", action="store_true", help="Only generate training data without training")
    parser.add_argument("--enrich", action="store_true", help="Enrich existing training data instead of replacing it")
    parser.add_argument("--augment", action="store_true", help="Apply data augmentation techniques")
    parser.add_argument("--output", type=str, default="training/generated_training_data.json",
                        help="Output path for generated training data")
    parser.add_argument("--model-output", type=str, default="models/intent_classifier",
                        help="Output directory for trained model")
    parser.add_argument("--cross-validation", type=int, default=0,
                        help="Number of cross-validation folds (0 to disable)")
    parser.add_argument("--early-stopping", action="store_true",
                        help="Enable early stopping based on validation loss")
    parser.add_argument("--patience", type=int, default=5,
                        help="Patience for early stopping (epochs with no improvement)")
    parser.add_argument("--debug", action="store_true", help="Show detailed debug logs")
    parser.add_argument("--reduce-complexity", action="store_true",
                        help="Use a simpler model architecture")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    db_connector = None

    try:
        try:
            from database_connector import DatabaseConnector
            from training.query_generator import DatabaseQueryGenerator
            from model.intent_classifier import IntentClassifier
            import tensorflow as tf
            logger.info(f"TensorFlow version: {tf.__version__}")
        except ImportError as e:
            logger.error(f"Import error: {e}")
            logger.error("Make sure all required modules are installed and in the Python path")
            return False

        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading configuration file: {e}")
            return False

        logger.info("Connecting to database...")
        db_config = config.get("database", {})

        if not db_config:
            logger.error("Database configuration not found in config file")
            return False

        db_connector = DatabaseConnector(
            host=db_config.get("host", "localhost"),
            user=db_config.get("user", ""),
            password=db_config.get("password", ""),
            database=db_config.get("database", "")
        )

        connected = db_connector.connect()
        if not connected:
            logger.error("Failed to connect to database. Exiting.")
            return False

        logger.info("Initializing query generator...")
        query_generator = DatabaseQueryGenerator(
            db_connector=db_connector,
            output_path=args.output
        )

        logger.info("Generating training data from database schema...")
        if args.enrich:
            training_data = query_generator.enrich_existing_training_data()
            training_path = args.output.replace('.json', '_combined.json')
            logger.info(f"Using enriched training data: {training_path}")
        else:
            training_data = query_generator.generate_training_data()
            training_path = args.output
            logger.info(f"Using generated training data: {training_path}")

        if not training_data or not training_data.get('texts'):
            logger.warning("No training data was generated")
            return False

        texts = training_data["texts"]
        labels = training_data["labels"]

        logger.info(f"Generated {len(texts)} training examples")

        intent_counts = {}
        for label in labels:
            intent_counts[label] = intent_counts.get(label, 0) + 1

        logger.info("Intent distribution:")
        for intent, count in intent_counts.items():
            percentage = count / len(labels) * 100 if labels else 0
            logger.info(f"  {intent}: {count} examples ({percentage:.1f}%)")

        if args.augment:
            logger.info("Applying data augmentation...")
            try:
                texts, labels = augment_text_data_simple(texts, labels)
            except Exception as e:
                logger.warning(f"Data augmentation failed: {e}")
                logger.warning("Continuing without augmentation")

            intent_counts = {}
            for label in labels:
                intent_counts[label] = intent_counts.get(label, 0) + 1

            logger.info("Intent distribution after augmentation:")
            for intent, count in intent_counts.items():
                percentage = count / len(labels) * 100 if labels else 0
                logger.info(f"  {intent}: {count} examples ({percentage:.1f}%)")

        if args.generate_only:
            logger.info("Training data generation completed. Exiting without training.")
            if db_connector:
                db_connector.handle_unread_results()
                db_connector.disconnect()
            return True

        unique_intents = sorted(set(labels))
        num_intent_classes = len(unique_intents)
        logger.info(f"Detected {num_intent_classes} unique intent classes: {unique_intents}")

        training_config = config.get("model", {}).get("training", {})
        epochs = training_config.get("epochs", 20)
        batch_size = training_config.get("batch_size", 32)
        validation_split = training_config.get("validation_split", 0.2)

        model_config = config.get("model", {}).get("parameters", {})
        vocab_size = model_config.get("vocab_size", 5000)
        embedding_dim = model_config.get("embedding_dim", 128)
        max_sequence_length = model_config.get("max_sequence_length", 50)

        if args.reduce_complexity:
            logger.info("Using simplified model architecture")
            embedding_dim = 64
            lstm_units = 32
            l2_factor = 0.01
        else:
            lstm_units = 64
            l2_factor = 0.001

        from tensorflow.keras.preprocessing.text import Tokenizer
        from tensorflow.keras.preprocessing.sequence import pad_sequences

        tokenizer = Tokenizer(num_words=vocab_size)
        tokenizer.fit_on_texts(texts)

        sequences = tokenizer.texts_to_sequences(texts)

        padded_sequences = pad_sequences(
            sequences,
            maxlen=max_sequence_length,
            padding='post'
        )

        label_indices = [unique_intents.index(label) for label in labels]

        from tensorflow.keras.utils import to_categorical
        one_hot_labels = to_categorical(label_indices, num_classes=num_intent_classes)

        callbacks = []

        if args.early_stopping:
            early_stopping = EarlyStopping(
                monitor='val_loss',
                patience=args.patience,
                restore_best_weights=True,
                verbose=1
            )
            callbacks.append(early_stopping)

            reduce_lr = ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=args.patience // 2,
                min_lr=1e-6,
                verbose=1
            )
            callbacks.append(reduce_lr)

        checkpoint = ModelCheckpoint(
            os.path.join(args.model_output, "best_model.h5"),
            monitor='val_loss',
            save_best_only=True,
            verbose=1
        )
        callbacks.append(checkpoint)

        if args.cross_validation > 1:
            logger.info(f"Performing {args.cross_validation}-fold cross-validation")

            kf = KFold(n_splits=args.cross_validation, shuffle=True, random_state=42)
            fold_results = []

            for fold, (train_idx, val_idx) in enumerate(kf.split(padded_sequences)):
                logger.info(f"Training fold {fold + 1}/{args.cross_validation}")

                X_train = padded_sequences[train_idx]
                y_train = one_hot_labels[train_idx]
                X_val = padded_sequences[val_idx]
                y_val = one_hot_labels[val_idx]

                intent_classifier = IntentClassifier(
                    vocab_size=vocab_size,
                    embedding_dim=embedding_dim,
                    max_sequence_length=max_sequence_length
                )

                model = intent_classifier.build_regularized_model(
                    num_intent_classes=num_intent_classes,
                    lstm_units=lstm_units,
                    dropout_rate=0.5,
                    recurrent_dropout=0.2,
                    l2_factor=l2_factor
                )

                history = model.fit(
                    X_train, y_train,
                    validation_data=(X_val, y_val),
                    epochs=epochs,
                    batch_size=batch_size,
                    callbacks=callbacks,
                    verbose=1
                )

                val_loss, val_acc = model.evaluate(X_val, y_val, verbose=0)
                fold_results.append((val_loss, val_acc))

                model.save(os.path.join(args.model_output, f"fold_{fold + 1}_model.h5"))

            mean_val_loss = np.mean([res[0] for res in fold_results])
            mean_val_acc = np.mean([res[1] for res in fold_results])
            logger.info(
                f"Cross-validation results: Mean val_loss = {mean_val_loss:.4f}, Mean val_acc = {mean_val_acc:.4f}")

            logger.info("Training final model on all data...")

        else:

            logger.info("Initializing intent classifier...")
            intent_classifier = IntentClassifier(
                vocab_size=vocab_size,
                embedding_dim=embedding_dim,
                max_sequence_length=max_sequence_length
            )
            intent_classifier.tokenizer = tokenizer
            intent_classifier.intent_classes = unique_intents

            model = intent_classifier.build_regularized_model(
                num_intent_classes=num_intent_classes,
                lstm_units=lstm_units,
                dropout_rate=0.5,
                recurrent_dropout=0.2,
                l2_factor=l2_factor
            )

            logger.info(f"Starting model training with {epochs} epochs and batch size {batch_size}...")

            from sklearn.model_selection import train_test_split

            X_train, X_val, y_train, y_val = train_test_split(
                padded_sequences,
                one_hot_labels,
                test_size=validation_split,
                stratify=label_indices,
                random_state=42
            )

            history = model.fit(
                X_train, y_train,
                validation_data=(X_val, y_val),
                epochs=epochs,
                batch_size=batch_size,
                callbacks=callbacks,
                verbose=1
            )

            intent_classifier.model = model

            logger.info(f"Training completed. Saving model to {args.model_output}...")
            os.makedirs(args.model_output, exist_ok=True)
            intent_classifier.save_model(args.model_output)

            history_path = os.path.join(args.model_output, "training_history.json")
            with open(history_path, 'w') as f:
                serializable_history = {}
                for key, values in history.history.items():
                    serializable_history[key] = [float(v) for v in values]
                json.dump(serializable_history, f, indent=2)

            logger.info(f"Training history saved to {history_path}")

            try:
                final_epoch = len(history.history['accuracy']) - 1
                logger.info(f"Final training accuracy: {history.history['accuracy'][final_epoch]:.4f}")
                logger.info(f"Final validation accuracy: {history.history['val_accuracy'][final_epoch]:.4f}")
            except (KeyError, IndexError) as e:
                logger.warning(f"Could not log metrics: {e}")
                logger.info("Training completed but metrics could not be displayed")

        return True

    except Exception as e:
        logger.error(f"Error in training process: {e}", exc_info=True)
        return False
    finally:
        if db_connector:
            try:
                db_connector.handle_unread_results()
                db_connector.disconnect()
            except Exception as e:
                logger.error(f"Error during database cleanup: {e}")


if __name__ == "__main__":
    start_time = datetime.now()
    logger.info(f"=== Starting enhanced training process at {start_time} ===")

    success = main()

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"=== Training process {'completed successfully' if success else 'failed'} in {duration} ===")