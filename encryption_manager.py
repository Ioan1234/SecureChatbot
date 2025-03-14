
import logging
import numpy as np

try:
    import tenseal as ts
except ImportError:
    logging.error("TenSEAL not installed. Please install it with: pip install tenseal")


class HomomorphicEncryptionManager:
    def __init__(self, key_size=2048, context_params=None):
        self.logger = logging.getLogger(__name__)
        self.key_size = key_size
        self.context_params = context_params or {}
        self.context = None
        self.public_key = None
        self.private_key = None
        self.setup_context()

    def setup_context(self):
        try:
            poly_modulus_degree = self.context_params.get("poly_modulus_degree", 8192)
            coeff_mod_bit_sizes = self.context_params.get("coeff_mod_bit_sizes", [60, 40, 40, 60])
            scale_bits = self.context_params.get("scale", 40)

            self.context = ts.context(
                ts.SCHEME_TYPE.CKKS,
                poly_modulus_degree=poly_modulus_degree,
                coeff_mod_bit_sizes=coeff_mod_bit_sizes
            )
            self.context.global_scale = 2 ** scale_bits
            self.context.generate_galois_keys()

            self.secret_context = self.context.copy()

            self.context.make_context_public()

            self.logger.info("Homomorphic encryption context initialized")
        except Exception as e:
            self.logger.error(f"Error initializing homomorphic encryption: {e}")

    def encrypt_vector(self, vector):
        try:
            encrypted_vector = ts.ckks_vector(self.context, vector)
            return encrypted_vector
        except Exception as e:
            self.logger.error(f"Error encrypting vector: {e}")
            return None

    def decrypt_vector(self, encrypted_vector):
        try:
            decrypted_vector = encrypted_vector.decrypt(self.secret_context)
            return decrypted_vector
        except Exception as e:
            self.logger.error(f"Error decrypting vector: {e}")
            return None

    def encrypt_data(self, data):
        if isinstance(data, (int, float)):
            return self.encrypt_vector([data])
        elif isinstance(data, str):
            ascii_values = [ord(c) for c in data]
            return self.encrypt_vector(ascii_values)
        elif isinstance(data, list):
            return self.encrypt_vector(data)
        else:
            self.logger.error(f"Unsupported data type for encryption: {type(data)}")
            return None

    def perform_encrypted_operation(self, operation, *encrypted_values):
        if not encrypted_values:
            return None

        try:
            result = encrypted_values[0]
            for value in encrypted_values[1:]:
                if operation == "add":
                    result = result + value
                elif operation == "multiply":
                    result = result * value
                elif operation == "dot_product":
                    result = result.dot_product(value)
                else:
                    self.logger.error(f"Unsupported operation: {operation}")
                    return None
            return result
        except Exception as e:
            self.logger.error(f"Error performing encrypted operation: {e}")
            return None