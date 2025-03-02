# training/trainer.py
import tensorflow as tf
import logging
import os


class DistributedTrainer:
    def __init__(self):
        """Initialize training for single machine with potential multiple GPUs"""
        self.logger = logging.getLogger(__name__)
        # Configure GPU memory growth to avoid OOM errors
        self._configure_gpus()
        self.strategy = None
        self.setup_strategy()

    def _configure_gpus(self):
        """Configure GPUs before setting up strategy"""
        try:
            # Check if GPUs have already been initialized
            if tf.config.experimental.get_memory_growth is None:
                # List available GPUs and configure them
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    self.logger.info(f"Found {len(gpus)} GPU(s): {gpus}")

                    # Configure memory growth for each GPU
                    for gpu in gpus:
                        try:
                            tf.config.experimental.set_memory_growth(gpu, True)
                            self.logger.info(f"Enabled memory growth for {gpu}")
                        except RuntimeError as e:
                            # Memory growth must be set before GPUs have been initialized
                            self.logger.warning(f"Could not set memory growth for {gpu}: {e}")

                    # Log GPU info
                    self.logger.info(f"Using TensorFlow version: {tf.__version__}")
                    self.logger.info(
                        f"CUDA visible devices: {os.environ.get('CUDA_VISIBLE_DEVICES', 'Not explicitly set')}")

                    # Optional: Set a simple test tensor on GPU to verify it works
                    try:
                        with tf.device('/GPU:0'):
                            test_tensor = tf.constant([1, 2, 3])
                            self.logger.info(f"Test tensor placed on GPU: {test_tensor.device}")
                    except RuntimeError as e:
                        self.logger.warning(f"Could not place test tensor on GPU: {e}")
                else:
                    self.logger.warning("No GPU detected by TensorFlow")
            else:
                self.logger.warning("GPUs have already been initialized, skipping memory growth configuration")
        except Exception as e:
            self.logger.error(f"Error configuring GPUs: {e}")

    def setup_strategy(self):
        """Set up TensorFlow distribution strategy for local GPUs"""
        try:
            # Log available devices before creating strategy
            physical_devices = tf.config.list_physical_devices()
            self.logger.info(f"Available physical devices: {physical_devices}")

            # For multi-GPU single machine
            self.strategy = tf.distribute.MirroredStrategy()
            self.logger.info(f"MirroredStrategy set up with {self.strategy.num_replicas_in_sync} devices")

            # If no GPUs available, this will use CPU
            if self.strategy.num_replicas_in_sync == 1:
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    self.logger.info("Strategy using single GPU for training")
                else:
                    self.logger.info("No GPUs detected. Using CPU for training.")
        except Exception as e:
            self.logger.error(f"Error setting up distribution strategy: {e}")
            self.strategy = None

    # Rest of the class remains the same...

    def build_distributed_model(self, model_fn, *args, **kwargs):
        """Build a model using the distribution strategy"""
        if not self.strategy:
            self.logger.error("No distribution strategy available")
            return None

        try:
            with self.strategy.scope():
                model = model_fn(*args, **kwargs)
            return model
        except Exception as e:
            self.logger.error(f"Error building distributed model: {e}")
            return None

    def load_training_data(self, training_data_path):
        """Load training data from a JSON file

        Args:
            training_data_path: Path to training data JSON file

        Returns:
            tuple: (texts, labels) for training
        """
        try:
            import json
            with open(training_data_path, 'r') as f:
                training_data = json.load(f)

            texts = training_data.get("texts", [])
            labels = training_data.get("labels", [])

            if not texts or not labels or len(texts) != len(labels):
                self.logger.error(
                    f"Invalid training data format. Expected 'texts' and 'labels' arrays of equal length.")
                return None, None

            self.logger.info(f"Loaded {len(texts)} training examples")
            return texts, labels
        except Exception as e:
            self.logger.error(f"Error loading training data: {e}")
            return None, None

    def train_intent_classifier(self, intent_classifier, training_data_path, epochs=20, batch_size=32,
                                validation_split=0.2):
        """Train an IntentClassifier

        Args:
            intent_classifier: IntentClassifier instance
            training_data_path: Path to training data JSON file
            epochs: Number of training epochs
            batch_size: Batch size for training
            validation_split: Fraction of data to use for validation

        Returns:
            training history or None if training failed
        """
        if not self.strategy:
            self.logger.error("No distribution strategy available")
            return None

        try:
            # Log training device
            self.logger.info(f"Training using: {tf.config.list_physical_devices()}")

            # Load training data
            texts, labels = self.load_training_data(training_data_path)
            if texts is None or labels is None:
                return None

            # Train the model within strategy scope
            with self.strategy.scope():
                # Log training start with GPU info
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    self.logger.info(f"Training on GPU: {gpus}")

                history = intent_classifier.train(
                    texts=texts,
                    labels=labels,
                    validation_split=validation_split,
                    epochs=epochs,
                    batch_size=batch_size
                )

            self.logger.info("Training completed successfully")
            return history
        except Exception as e:
            self.logger.error(f"Error in training: {e}")
            return None

    def save_model(self, model, model_dir="models/intent_classifier"):
        """Save the model

        Args:
            model: Model to save
            model_dir: Directory to save the model to
        """
        try:
            os.makedirs(model_dir, exist_ok=True)
            model.save_model(model_dir)
            self.logger.info(f"Model saved to {model_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
            return False

    def distribute_training(self, model, dataset, epochs=10, steps_per_epoch=None):
        """Train model with dataset using local GPUs if available"""
        if not self.strategy:
            self.logger.error("No distribution strategy available")
            return None

        try:
            # Create distributed dataset
            dist_dataset = self.strategy.experimental_distribute_dataset(dataset)

            # Define training step function
            @tf.function
            def train_step(dist_inputs):
                def step_fn(inputs):
                    features, labels = inputs
                    with tf.GradientTape() as tape:
                        predictions = model(features, training=True)
                        loss = model.compiled_loss(labels, predictions)

                    gradients = tape.gradient(loss, model.trainable_variables)
                    model.optimizer.apply_gradients(zip(gradients, model.trainable_variables))
                    model.compiled_metrics.update_state(labels, predictions)

                    return {m.name: m.result() for m in model.metrics}

                return self.strategy.run(step_fn, args=(dist_inputs,))

            # Log the device placement
            self.logger.info(f"Training with device placement: {tf.config.list_physical_devices()}")

            # Training loop
            for epoch in range(epochs):
                self.logger.info(f"Epoch {epoch + 1}/{epochs}")
                results = {}

                for step, dist_inputs in enumerate(dist_dataset):
                    if steps_per_epoch and step >= steps_per_epoch:
                        break

                    step_results = train_step(dist_inputs)

                    # Aggregate results
                    for name, result in step_results.items():
                        if name not in results:
                            results[name] = []
                        results[name].append(result)

                # Print epoch results
                self.logger.info(f"Epoch {epoch + 1} results:")
                for name, values in results.items():
                    self.logger.info(f"  {name}: {sum(values) / len(values)}")

            return results
        except Exception as e:
            self.logger.error(f"Error in training: {e}")
            return None