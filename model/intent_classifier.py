import os
import numpy as np
import logging
import json
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model


class IntentClassifier:
    def __init__(self, vocab_size=5000, embedding_dim=128, max_sequence_length=50):
        self.logger = logging.getLogger(__name__)
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.max_sequence_length = max_sequence_length
        self.model = None
        self.tokenizer = None
        self.intent_classes = []

    def build_model(self, num_intent_classes):

        model = tf.keras.Sequential([
            tf.keras.layers.Embedding(self.vocab_size, self.embedding_dim, input_length=self.max_sequence_length),
            tf.keras.layers.LSTM(64, dropout=0.2, recurrent_dropout=0.2),
            tf.keras.layers.Dense(num_intent_classes, activation='softmax')
        ])

        model.compile(
            loss='categorical_crossentropy',
            optimizer='adam',
            metrics=['accuracy']
        )

        self.model = model
        return model

    def build_regularized_model(self, num_intent_classes, lstm_units=64, dropout_rate=0.5, recurrent_dropout=0.2,
                                l2_factor=0.001):
        regularizer = tf.keras.regularizers.l2(l2_factor)

        model = tf.keras.Sequential([
            tf.keras.layers.Embedding(
                self.vocab_size,
                self.embedding_dim,
                input_length=self.max_sequence_length,
                embeddings_regularizer=regularizer
            ),
            tf.keras.layers.Bidirectional(
                tf.keras.layers.LSTM(
                    lstm_units,
                    dropout=dropout_rate,
                    recurrent_dropout=recurrent_dropout,
                    return_sequences=True,
                    kernel_regularizer=regularizer
                )
            ),
            tf.keras.layers.Bidirectional(
                tf.keras.layers.LSTM(
                    lstm_units,
                    dropout=dropout_rate,
                    recurrent_dropout=recurrent_dropout,
                    kernel_regularizer=regularizer
                )
            ),
            tf.keras.layers.Dense(64, activation='relu', kernel_regularizer=regularizer),
            tf.keras.layers.Dropout(dropout_rate),
            tf.keras.layers.Dense(num_intent_classes, activation='softmax')
        ])

        model.compile(
            loss='categorical_crossentropy',
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
            metrics=['accuracy']
        )

        self.model = model
        return model

    def classify_intent(self, query):

        try:
            if self.model is None or self.tokenizer is None:
                self.logger.error("Model or tokenizer not initialized")
                return {"intent": "database_query_list", "confidence": 0.5}

            sequence = self.tokenizer.texts_to_sequences([query])
            padded_sequence = pad_sequences(sequence, maxlen=self.max_sequence_length, padding='post')

            prediction = self.model.predict(padded_sequence, verbose=0)[0]
            intent_index = np.argmax(prediction)
            confidence = float(prediction[intent_index])

            if intent_index < len(self.intent_classes):
                intent = self.intent_classes[intent_index]
            else:
                self.logger.warning(f"Intent index {intent_index} out of range for intent classes")
                intent = "database_query_list"  # Default to database query

            return {
                "intent": intent,
                "confidence": confidence
            }
        except Exception as e:
            self.logger.error(f"Error classifying intent: {e}")
            return {"intent": "database_query_list", "confidence": 0.5}

    def save_model(self, model_dir):

        try:
            if not os.path.exists(model_dir):
                os.makedirs(model_dir)

            if self.model:
                model_path = os.path.join(model_dir, "intent_model.h5")
                self.model.save(model_path)
                self.logger.info(f"Model saved to {model_path}")

            if self.tokenizer:
                tokenizer_path = os.path.join(model_dir, "tokenizer.json")
                tokenizer_json = self.tokenizer.to_json()
                with open(tokenizer_path, 'w') as f:
                    f.write(tokenizer_json)
                self.logger.info(f"Tokenizer saved to {tokenizer_path}")

            if self.intent_classes:
                classes_path = os.path.join(model_dir, "intent_classes.json")
                with open(classes_path, 'w') as f:
                    json.dump(self.intent_classes, f)
                self.logger.info(f"Intent classes saved to {classes_path}")

            config_path = os.path.join(model_dir, "model_config.json")
            config = {
                "vocab_size": self.vocab_size,
                "embedding_dim": self.embedding_dim,
                "max_sequence_length": self.max_sequence_length
            }
            with open(config_path, 'w') as f:
                json.dump(config, f)
            self.logger.info(f"Model configuration saved to {config_path}")

            return True
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
            return False

    def load_model(self, model_dir):

        try:
            config_path = os.path.join(model_dir, "model_config.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                self.vocab_size = config.get("vocab_size", 5000)
                self.embedding_dim = config.get("embedding_dim", 128)
                self.max_sequence_length = config.get("max_sequence_length", 50)
                self.logger.info(f"Loaded model configuration from {config_path}")

            classes_path = os.path.join(model_dir, "intent_classes.json")
            if os.path.exists(classes_path):
                with open(classes_path, 'r') as f:
                    self.intent_classes = json.load(f)
                self.logger.info(f"Loaded {len(self.intent_classes)} intent classes from {classes_path}")

            tokenizer_path = os.path.join(model_dir, "tokenizer.json")
            if os.path.exists(tokenizer_path):
                with open(tokenizer_path, 'r') as f:
                    tokenizer_json = f.read()
                self.tokenizer = tf.keras.preprocessing.text.tokenizer_from_json(tokenizer_json)
                self.logger.info(f"Loaded tokenizer from {tokenizer_path}")

            model_path = os.path.join(model_dir, "intent_model.h5")
            if os.path.exists(model_path):
                self.model = load_model(model_path)
                self.logger.info(f"Loaded model from {model_path}")

            return True
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return False