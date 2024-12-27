import tensorflow as tf
import os

tf_config = os.environ.get("TF_CONFIG")
print("TF_CONFIG:", tf_config)

strategy = tf.distribute.MultiWorkerMirroredStrategy()

def build_and_run():
    with strategy.scope():
        x = tf.constant(5.0)
        y = tf.constant(3.0)
        z = x + y
    return z

result = build_and_run()
print("Result of distributed computation:", result)
