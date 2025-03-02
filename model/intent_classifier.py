import tensorflow as tf
import numpy as np
import os
import json
import logging


class IntentClassifier:
    def __init__(self, vocab_size=5000, embedding_dim=128, max_sequence_length=50, model_path=None):
        """Initialize the intent classifier model"""
        self.logger = logging.getLogger(__name__)
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.max_sequence_length = max_sequence_length
        self.model_path = model_path
        self.model = None
        self.tokenizer = None
        self.intent_classes = []

        if model_path and os.path.exists(f"{model_path}/model.h5"):
            self.load_model(model_path)
        else:
            self.build_model()

    def build_model(self, num_intent_classes=10):
        """Build the neural network architecture"""
        try:
            # Input layer
            input_layer = tf.keras.layers.Input(shape=(self.max_sequence_length,))

            # Embedding layer
            embedding_layer = tf.keras.layers.Embedding(
                input_dim=self.vocab_size,
                output_dim=self.embedding_dim,
                input_length=self.max_sequence_length
            )(input_layer)

            # LSTM layers
            lstm_layer = tf.keras.layers.Bidirectional(
                tf.keras.layers.LSTM(64, return_sequences=True)
            )(embedding_layer)
            lstm_layer = tf.keras.layers.Bidirectional(
                tf.keras.layers.LSTM(32)
            )(lstm_layer)

            # Dense layers
            dense_layer = tf.keras.layers.Dense(64, activation='relu')(lstm_layer)
            dropout_layer = tf.keras.layers.Dropout(0.5)(dense_layer)

            # Output layer
            output_layer = tf.keras.layers.Dense(num_intent_classes, activation='softmax')(dropout_layer)

            # Create model
            self.model = tf.keras.Model(inputs=input_layer, outputs=output_layer)

            # Compile model
            self.model.compile(
                loss='categorical_crossentropy',
                optimizer='adam',
                metrics=['accuracy']
            )

            self.logger.info("Intent classification model built successfully")
            return self.model
        except Exception as e:
            self.logger.error(f"Error building intent classification model: {e}")
            return None

    def train(self, texts, labels, validation_split=0.2, epochs=20, batch_size=32):
        """Train the intent classification model"""
        try:
            # Create tokenizer
            self.tokenizer = tf.keras.preprocessing.text.Tokenizer(num_words=self.vocab_size)
            self.tokenizer.fit_on_texts(texts)

            # Convert texts to sequences
            sequences = self.tokenizer.texts_to_sequences(texts)

            # Pad sequences
            padded_sequences = tf.keras.preprocessing.sequence.pad_sequences(
                sequences, maxlen=self.max_sequence_length, padding='post'
            )

            # Get unique intent classes
            self.intent_classes = sorted(list(set(labels)))
            num_intent_classes = len(self.intent_classes)

            # Build model if not already built
            if self.model is None:
                self.build_model(num_intent_classes)

            # Convert labels to one-hot encoding
            label_indices = [self.intent_classes.index(label) for label in labels]
            one_hot_labels = tf.keras.utils.to_categorical(label_indices, num_classes=num_intent_classes)

            # Train model
            history = self.model.fit(
                padded_sequences,
                one_hot_labels,
                validation_split=validation_split,
                epochs=epochs,
                batch_size=batch_size
            )

            self.logger.info("Intent classification model trained successfully")
            return history
        except Exception as e:
            self.logger.error(f"Error training intent classification model: {e}")
            return None

    def save_model(self, directory_path):
        """Save the model and its components"""
        try:
            os.makedirs(directory_path, exist_ok=True)

            # Save model
            self.model.save(f"{directory_path}/model.h5")

            # Save tokenizer
            tokenizer_json = self.tokenizer.to_json()
            with open(f"{directory_path}/tokenizer.json", 'w') as f:
                f.write(tokenizer_json)

            # Save intent classes
            with open(f"{directory_path}/intent_classes.json", 'w') as f:
                json.dump(self.intent_classes, f)

            # Save model parameters
            params = {
                'vocab_size': self.vocab_size,
                'embedding_dim': self.embedding_dim,
                'max_sequence_length': self.max_sequence_length
            }
            with open(f"{directory_path}/params.json", 'w') as f:
                json.dump(params, f)

            self.logger.info(f"Model saved to {directory_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
            return False

    def load_model(self, directory_path):
        """Load the model and its components"""
        try:
            # Load model
            self.model = tf.keras.models.load_model(f"{directory_path}/model.h5")

            # Load tokenizer
            with open(f"{directory_path}/tokenizer.json", 'r') as f:
                tokenizer_json = f.read()
                self.tokenizer = tf.keras.preprocessing.text.tokenizer_from_json(tokenizer_json)

            # Load intent classes
            with open(f"{directory_path}/intent_classes.json", 'r') as f:
                self.intent_classes = json.load(f)

            # Load model parameters
            with open(f"{directory_path}/params.json", 'r') as f:
                params = json.load(f)
                self.vocab_size = params['vocab_size']
                self.embedding_dim = params['embedding_dim']
                self.max_sequence_length = params['max_sequence_length']

            self.logger.info(f"Model loaded from {directory_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return False

    def classify_intent(self, text):
        """Classify the intent of a text"""
        try:
            # Convert text to sequence
            sequence = self.tokenizer.texts_to_sequences([text])

            # Pad sequence
            padded_sequence = tf.keras.preprocessing.sequence.pad_sequences(
                sequence, maxlen=self.max_sequence_length, padding='post'
            )

            # Predict
            prediction = self.model.predict(padded_sequence)[0]

            # Get intent class with highest probability
            intent_index = np.argmax(prediction)
            intent = self.intent_classes[intent_index]
            confidence = float(prediction[intent_index])

            return {
                'intent': intent,
                'confidence': confidence
            }
        except Exception as e:
            self.logger.error(f"Error classifying intent: {e}")
            return None