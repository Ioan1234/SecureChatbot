import tensorflow as tf
import numpy as np
import os
import json
import logging
from tensorflow.keras.layers import Input, Embedding, LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.models import Model
from tensorflow.keras.regularizers import l2
from tensorflow.keras.optimizers import Adam


class IntentClassifier:
    def __init__(self, vocab_size=5000, embedding_dim=128, max_sequence_length=50, model_path=None):
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
        try:
            input_layer = Input(shape=(self.max_sequence_length,))

            embedding_layer = Embedding(
                input_dim=self.vocab_size,
                output_dim=self.embedding_dim,
                input_length=self.max_sequence_length
            )(input_layer)

            lstm_layer = Bidirectional(
                LSTM(64, return_sequences=True)
            )(embedding_layer)
            lstm_layer = Bidirectional(
                LSTM(32)
            )(lstm_layer)

            dense_layer = Dense(64, activation='relu')(lstm_layer)
            dropout_layer = Dropout(0.5)(dense_layer)

            output_layer = Dense(num_intent_classes, activation='softmax')(dropout_layer)

            self.model = Model(inputs=input_layer, outputs=output_layer)

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

    def build_regularized_model(self, num_intent_classes=10, lstm_units=64, dropout_rate=0.5, recurrent_dropout=0.2,
                                l2_factor=0.001):
        try:
            input_layer = Input(shape=(self.max_sequence_length,))

            embedding_layer = Embedding(
                input_dim=self.vocab_size,
                output_dim=self.embedding_dim,
                input_length=self.max_sequence_length,
                embeddings_regularizer=l2(l2_factor)
            )(input_layer)

            embedding_dropout = Dropout(dropout_rate / 2)(embedding_layer)

            lstm_layer = Bidirectional(
                LSTM(lstm_units,
                     return_sequences=True,
                     dropout=dropout_rate,
                     recurrent_dropout=recurrent_dropout,
                     kernel_regularizer=l2(l2_factor),
                     recurrent_regularizer=l2(l2_factor / 2))
            )(embedding_dropout)

            lstm_dropout = Dropout(dropout_rate)(lstm_layer)

            lstm_layer = Bidirectional(
                LSTM(lstm_units // 2,
                     dropout=dropout_rate,
                     recurrent_dropout=recurrent_dropout,
                     kernel_regularizer=l2(l2_factor),
                     recurrent_regularizer=l2(l2_factor / 2))
            )(lstm_dropout)

            dense_layer = Dense(
                lstm_units,
                activation='relu',
                kernel_regularizer=l2(l2_factor)
            )(lstm_layer)

            dropout_layer = Dropout(dropout_rate)(dense_layer)

            output_layer = Dense(
                num_intent_classes,
                activation='softmax',
                kernel_regularizer=l2(l2_factor / 2)
            )(dropout_layer)

            self.model = Model(inputs=input_layer, outputs=output_layer)

            self.model.compile(
                loss='categorical_crossentropy',
                optimizer=Adam(learning_rate=0.001),
                metrics=['accuracy']
            )

            self.logger.info("Regularized intent classification model built successfully")
            return self.model
        except Exception as e:
            self.logger.error(f"Error building regularized intent classification model: {e}")
            return None

    def train(self, texts, labels, validation_split=0.2, epochs=20, batch_size=32):
        try:
            if self.tokenizer is None:
                self.tokenizer = tf.keras.preprocessing.text.Tokenizer(num_words=self.vocab_size)
                self.tokenizer.fit_on_texts(texts)

            sequences = self.tokenizer.texts_to_sequences(texts)

            padded_sequences = tf.keras.preprocessing.sequence.pad_sequences(
                sequences, maxlen=self.max_sequence_length, padding='post'
            )

            self.intent_classes = sorted(list(set(labels)))
            num_intent_classes = len(self.intent_classes)

            if self.model is None or self.model.output_shape[-1] != num_intent_classes:
                self.logger.info(f"Building model with {num_intent_classes} output classes")
                self.build_model(num_intent_classes)

            label_indices = [self.intent_classes.index(label) for label in labels]
            one_hot_labels = tf.keras.utils.to_categorical(label_indices, num_classes=num_intent_classes)

            from sklearn.model_selection import train_test_split

            X_train, X_val, y_train, y_val = train_test_split(
                padded_sequences,
                one_hot_labels,
                test_size=validation_split,
                stratify=label_indices,
                random_state=42
            )

            callbacks = [
                tf.keras.callbacks.EarlyStopping(
                    monitor='val_loss',
                    patience=5,
                    restore_best_weights=True
                ),
                tf.keras.callbacks.ReduceLROnPlateau(
                    monitor='val_loss',
                    factor=0.5,
                    patience=2,
                    min_lr=1e-6
                )
            ]

            history = self.model.fit(
                X_train, y_train,
                validation_data=(X_val, y_val),
                epochs=epochs,
                batch_size=batch_size,
                callbacks=callbacks
            )

            self.logger.info("Intent classification model trained successfully")
            return history
        except Exception as e:
            self.logger.error(f"Error training intent classification model: {e}")
            return None

    def save_model(self, directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)

            self.model.save(f"{directory_path}/model.h5")

            if self.tokenizer:
                tokenizer_json = self.tokenizer.to_json()
                with open(f"{directory_path}/tokenizer.json", 'w') as f:
                    f.write(tokenizer_json)

            with open(f"{directory_path}/intent_classes.json", 'w') as f:
                json.dump(self.intent_classes, f)

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
        try:
            self.model = tf.keras.models.load_model(f"{directory_path}/model.h5")

            with open(f"{directory_path}/tokenizer.json", 'r') as f:
                tokenizer_json = f.read()
                self.tokenizer = tf.keras.preprocessing.text.tokenizer_from_json(tokenizer_json)

            with open(f"{directory_path}/intent_classes.json", 'r') as f:
                self.intent_classes = json.load(f)

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
        try:
            sequence = self.tokenizer.texts_to_sequences([text])

            padded_sequence = tf.keras.preprocessing.sequence.pad_sequences(
                sequence, maxlen=self.max_sequence_length, padding='post'
            )

            prediction = self.model.predict(padded_sequence)[0]

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