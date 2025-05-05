import os
import numpy as np
import logging
import json
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model


class EnhancedIntentClassifier:
    def __init__(self, vocab_size=5000, embedding_dim=128, max_sequence_length=50, use_post_processor=True):
        self.logger = logging.getLogger(__name__)
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.max_sequence_length = max_sequence_length
        self.model = None
        self.tokenizer = None
        self.intent_classes = []
        self.post_processor = None
        self.use_post_processor = use_post_processor
        self.model_dir = None

    def build_model(self, num_intent_classes):
        model = tf.keras.Sequential([
            tf.keras.layers.Embedding(self.vocab_size, self.embedding_dim, input_length=self.max_sequence_length),
            tf.keras.layers.Conv1D(128, 5, activation='relu'),
            tf.keras.layers.GlobalMaxPooling1D(),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.3),
            tf.keras.layers.Dense(num_intent_classes, activation='softmax')
        ])

        model.compile(
            loss='categorical_crossentropy',
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
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

    def build_multi_filter_model(self, num_intent_classes):

        inputs = tf.keras.layers.Input(shape=(self.max_sequence_length,))
        embedding = tf.keras.layers.Embedding(
            self.vocab_size, self.embedding_dim, input_length=self.max_sequence_length
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

        outputs = tf.keras.layers.Dense(num_intent_classes, activation='softmax')(dropout2)

        model = tf.keras.Model(inputs=inputs, outputs=outputs)

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

            self.logger.debug(f"Classifying query: '{query}'")
            self.logger.debug(f"Tokenizer word_index size: {len(self.tokenizer.word_index)}")

            sequence = self.tokenizer.texts_to_sequences([query])
            self.logger.debug(f"Sequence before padding: {sequence}")

            padded_sequence = pad_sequences(sequence, maxlen=self.max_sequence_length, padding='post')
            self.logger.debug(f"Padded sequence shape: {padded_sequence.shape}")

            try:
                prediction = self.model.predict(padded_sequence, verbose=0)[0]
                self.logger.debug(f"Raw prediction: {prediction}")
            except Exception as e:
                self.logger.error(f"Error during prediction: {e}")
                return {"intent": "database_query_list", "confidence": 0.5}

            intent_index = np.argmax(prediction)
            confidence = float(prediction[intent_index])

            self.logger.debug(f"Intent index: {intent_index}, available classes: {len(self.intent_classes)}")

            if intent_index < len(self.intent_classes):
                intent = self.intent_classes[intent_index]
            else:
                self.logger.warning(f"Intent index {intent_index} out of range for intent classes")
                intent = "database_query_list"

            result = {
                "intent": intent,
                "confidence": confidence
            }

            if self.use_post_processor and self.post_processor:
                try:
                    enhanced_result = self.post_processor.identify_sub_intent(query, intent, confidence)
                    self.logger.debug(f"Post-processed result: {enhanced_result}")


                    if enhanced_result.get("sub_intent") and enhanced_result.get("sub_confidence", 0) > 0.5:
                        result["sub_intent"] = enhanced_result["sub_intent"]
                        result["sub_confidence"] = enhanced_result["sub_confidence"]
                except Exception as e:
                    self.logger.warning(f"Error in post-processing: {e}")

            return result
        except Exception as e:
            self.logger.error(f"Error classifying intent: {e}")
            return {"intent": "database_query_list", "confidence": 0.5}

    def save_model(self, model_dir):
        try:
            if not os.path.exists(model_dir):
                os.makedirs(model_dir)

            if self.model:
                model_path = os.path.join(model_dir, "model.h5")
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
                "max_sequence_length": self.max_sequence_length,
                "use_post_processor": self.use_post_processor
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
            self.logger.info(f"Loading model from directory: {model_dir}")
            self.model_dir = model_dir

            config_path = os.path.join(model_dir, "model_config.json")
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                self.vocab_size = config.get("vocab_size", 5000)
                self.embedding_dim = config.get("embedding_dim", 128)
                self.max_sequence_length = config.get("max_sequence_length", 50)
                self.use_post_processor = config.get("use_post_processor", True)
                self.logger.info(f"Loaded model configuration from {config_path}")
                self.logger.info(
                    f"Configuration: vocab_size={self.vocab_size}, embedding_dim={self.embedding_dim}, max_sequence_length={self.max_sequence_length}, use_post_processor={self.use_post_processor}")
            else:
                self.logger.warning(f"No model config found at {config_path}, using defaults")

            classes_path = os.path.join(model_dir, "intent_classes.json")
            if os.path.exists(classes_path):
                try:
                    with open(classes_path, 'r') as f:
                        self.intent_classes = json.load(f)
                    self.logger.info(f"Loaded {len(self.intent_classes)} intent classes from {classes_path}")

                    if not self.intent_classes:
                        self.logger.warning("Intent classes file exists but is empty. Using default classes.")
                        self.intent_classes = ["database_query_list", "greeting", "help", "goodbye"]
                except Exception as e:
                    self.logger.error(f"Error reading intent classes file: {e}")
                    self.intent_classes = ["database_query_list", "greeting", "help", "goodbye"]
            else:
                self.logger.error(f"Intent classes file not found at {classes_path}")
                self.intent_classes = ["database_query_list", "greeting", "help", "goodbye"]

            self.logger.info(f"Using intent classes: {self.intent_classes}")

            tokenizer_path = os.path.join(model_dir, "tokenizer.json")
            if os.path.exists(tokenizer_path):
                try:
                    with open(tokenizer_path, 'r') as f:
                        tokenizer_json = f.read()
                    self.tokenizer = tf.keras.preprocessing.text.tokenizer_from_json(tokenizer_json)
                    self.logger.info(f"Loaded tokenizer from {tokenizer_path}")

                    if not hasattr(self.tokenizer, 'word_index') or not self.tokenizer.word_index:
                        self.logger.warning("Tokenizer has no word index. It may be empty.")
                        self.tokenizer = Tokenizer(num_words=self.vocab_size)
                        self.tokenizer.fit_on_texts(["hello world this is a test"])
                except Exception as e:
                    self.logger.error(f"Error loading tokenizer: {e}")
                    self.tokenizer = Tokenizer(num_words=self.vocab_size)
                    self.tokenizer.fit_on_texts(["hello world this is a test"])
            else:
                self.logger.error(f"Tokenizer file not found at {tokenizer_path}")
                self.tokenizer = Tokenizer(num_words=self.vocab_size)
                self.tokenizer.fit_on_texts(["hello world this is a test"])

            model_path = os.path.join(model_dir, "model.h5")
            if os.path.exists(model_path):
                try:
                    file_size = os.path.getsize(model_path)
                    if file_size < 1000:
                        self.logger.error(
                            f"Model file at {model_path} is too small ({file_size} bytes). It may be corrupted.")
                        self.model = None
                    else:
                        self.model = load_model(model_path)
                        self.logger.info(f"Loaded model from {model_path}")

                        if not isinstance(self.model, tf.keras.Model):
                            self.logger.error(f"Loaded object is not a valid Keras model. Type: {type(self.model)}")
                            self.model = None
                except Exception as e:
                    self.logger.error(f"Error loading model from {model_path}: {e}")
                    self.model = None
            else:
                alt_model_path = os.path.join(model_dir, "intent_model.h5")
                if os.path.exists(alt_model_path):
                    try:
                        self.logger.info(f"Model.h5 not found, trying intent_model.h5")
                        self.model = load_model(alt_model_path)
                        self.logger.info(f"Loaded model from {alt_model_path}")

                        self.model.save(model_path)
                        self.logger.info(f"Saved a copy as {model_path}")
                    except Exception as e:
                        self.logger.error(f"Error loading alternative model from {alt_model_path}: {e}")
                        self.model = None
                else:
                    self.logger.error(f"Model file not found at {model_path} or {alt_model_path}")
                    self.model = None


            if self.use_post_processor:
                try:
                    from intent_processor import IntentPostProcessor
                    self.post_processor = IntentPostProcessor(model_dir)
                    self.logger.info("Initialized intent post-processor")
                except Exception as e:
                    self.logger.warning(f"Could not initialize post-processor: {e}")
                    self.post_processor = None


            if self.model is None:
                self.logger.error("Model loading failed")
                return False

            if self.tokenizer is None:
                self.logger.error("Tokenizer loading failed")
                return False

            if not self.intent_classes:
                self.logger.error("Intent classes loading failed")
                return False

            self.logger.info("Model, tokenizer, and intent classes successfully loaded")
            return True
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return False