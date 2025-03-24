import logging
import os
import json
import numpy as np
import base64
from datetime import datetime

try:
    import tenseal as ts
except ImportError:
    logging.error("TenSEAL not installed. Please install it with: pip install tenseal")


class HomomorphicEncryptionManager:
    def __init__(self, key_size=2048, context_params=None, keys_dir="encryption_keys"):
        self.logger = logging.getLogger(__name__)
        self.key_size = key_size
        self.keys_dir = keys_dir

        os.makedirs(self.keys_dir, exist_ok=True)

        self.context_params = context_params or {
            "poly_modulus_degree": 8192,
            "coeff_mod_bit_sizes": [60, 40, 40, 60],
            "scale_bits": 40
        }

        self.sensitive_fields = {
            "traders.email": "string",
            "traders.phone": "string",
            "brokers.contact_email": "string",
            "brokers.license_number": "string",
            "accounts.balance": "numeric"
        }

        self.use_encryption = True

        self.context = None
        self.secret_context = None
        self.public_key = None
        self.private_key = None

        if self.use_encryption:
            self._initialize_encryption()

    def _initialize_encryption(self):
        try:
            if self._load_encryption_data():
                self.logger.info("Successfully loaded existing encryption data")
            else:
                self.logger.info("Creating new encryption context and keys")
                self._setup_new_encryption()
        except Exception as e:
            self.logger.error(f"Error initializing encryption: {e}")
            self._setup_simplified_encryption()

    def _load_encryption_data(self):
        try:
            context_path = os.path.join(self.keys_dir, "serialized_context.dat")
            key_path = os.path.join(self.keys_dir, "private_key.dat")

            if not os.path.exists(context_path) or not os.path.exists(key_path):
                self.logger.info("Encryption files not found, need to create new ones")
                return False

            self.logger.info("Loading encryption context from file")
            with open(context_path, 'rb') as f:
                serialized_context = f.read()

            self.context = ts.context_from(serialized_context)

            self.logger.info("Loading private key")
            with open(key_path, 'rb') as f:
                private_key_data = f.read()

            self.secret_context = ts.context_from(serialized_context)

            try:
                self.secret_context = ts.context_from(private_key_data)
                if not self.secret_context.is_private():
                    self.logger.error("Loaded secret context is not private")
                    return False
            except:
                self.logger.warning("Could not load private key with modern approach, trying legacy method")
                try:
                    self.secret_context.make_context_private(private_key_data)
                except:
                    self.logger.error("Failed to apply private key to context")
                    return False

            self.logger.info("Successfully loaded encryption context and private key")
            return True
        except Exception as e:
            self.logger.error(f"Error loading encryption data: {e}")
            return False

    def _setup_new_encryption(self):
        try:
            poly_modulus_degree = self.context_params.get("poly_modulus_degree", 8192)
            coeff_mod_bit_sizes = self.context_params.get("coeff_mod_bit_sizes", [60, 40, 40, 60])
            scale_bits = self.context_params.get("scale_bits", 40)

            self.logger.info("Creating new TenSEAL CKKS context")
            self.context = ts.context(
                ts.SCHEME_TYPE.CKKS,
                poly_modulus_degree=poly_modulus_degree,
                coeff_mod_bit_sizes=coeff_mod_bit_sizes
            )

            self.context.global_scale = 2 ** scale_bits

            self.context.generate_galois_keys()

            self.secret_context = self.context.copy()

            self._save_encryption_data()

            self.context.make_context_public()

            self.logger.info("Successfully created and saved new encryption context and keys")
            return True
        except Exception as e:
            self.logger.error(f"Error setting up new encryption: {e}")
            return False

    def _save_encryption_data(self):
        try:
            os.makedirs(self.keys_dir, exist_ok=True)

            context_path = os.path.join(self.keys_dir, "serialized_context.dat")
            with open(context_path, 'wb') as f:
                f.write(self.context.serialize())

            key_path = os.path.join(self.keys_dir, "private_key.dat")
            with open(key_path, 'wb') as f:
                f.write(self.secret_context.serialize())

            params_path = os.path.join(self.keys_dir, "context_params.json")
            with open(params_path, 'w') as f:
                json.dump(self.context_params, f)

            self.logger.info("Encryption data saved successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error saving encryption data: {e}")
            return False

    def _setup_simplified_encryption(self):
        self.logger.warning("Setting up simplified encryption as fallback")
        self.use_encryption = False
        self.context = None
        self.secret_context = None

        import secrets
        self.symmetric_key = secrets.token_bytes(32)

        key_path = os.path.join(self.keys_dir, "symmetric_key.dat")
        with open(key_path, 'wb') as f:
            f.write(self.symmetric_key)

    def encrypt_value(self, value, field_name=None):
        if value is None:
            return None

        field_type = self._get_field_type(field_name)

        try:
            if not self.use_encryption or not self.context:
                return self._simplified_encrypt(value, field_type)

            if field_type == "numeric":
                return self.encrypt_numeric(value)
            elif field_type == "string":
                return self.encrypt_string(value)
            else:
                return self.encrypt_string(value)
        except Exception as e:
            self.logger.error(f"Error encrypting value for field {field_name}: {e}")
            return self._simplified_encrypt(value, field_type)

    def decrypt_value(self, encrypted_value, field_name=None):
        if encrypted_value is None:
            return None

        if isinstance(encrypted_value, str) and encrypted_value.startswith("ENCRYPTION_ERROR_"):
            return None

        field_type = self._get_field_type(field_name)

        try:
            if not self.use_encryption or not self.secret_context:
                return self._simplified_decrypt(encrypted_value, field_type)

            if field_type == "numeric":
                return self.decrypt_numeric(encrypted_value)
            elif field_type == "string":
                return self.decrypt_string(encrypted_value)
            else:
                return self.decrypt_string(encrypted_value)
        except Exception as e:
            self.logger.error(f"Error decrypting value for field {field_name}: {e}")
            try:
                return self._simplified_decrypt(encrypted_value, field_type)
            except:
                return None

    def _get_field_type(self, field_name):
        if not field_name:
            return "string"

        return self.sensitive_fields.get(field_name, "string")

    def encrypt_numeric(self, value):
        try:
            if not self.secret_context or not self.context:
                raise ValueError("Encryption context not properly initialized")

            if not self.secret_context.is_private():
                raise ValueError("Secret context does not have a private key")

            value = float(value)

            encrypted_vector = ts.ckks_vector(self.context, [value])

            serialized = encrypted_vector.serialize()

            return serialized
        except Exception as e:
            self.logger.error(f"Error in numeric encryption: {e}")
            return self._simplified_encrypt(value, "numeric")

    def decrypt_numeric(self, encrypted_value):
        try:
            if not self.secret_context:
                self.logger.error("Secret context not available for decryption")
                raise ValueError("Secret context not available for decryption")

            if not self.secret_context.is_private():
                self.logger.error("Secret context does not have a private key")
                raise ValueError("Secret context does not have a private key")

            if isinstance(encrypted_value, bytes):
                try:
                    self.logger.info(f"Encrypted value length: {len(encrypted_value)} bytes")
                    self.logger.info(f"Encrypted value preview: {encrypted_value[:20]}")

                    encrypted_vector = ts.ckks_vector_from(self.secret_context, encrypted_value)

                    self.logger.info(f"Successfully deserialized encrypted vector")

                    decrypted_values = encrypted_vector.decrypt()
                    self.logger.info(f"Decrypted values: {decrypted_values}")

                    if decrypted_values and len(decrypted_values) > 0:
                        return decrypted_values[0]
                    return None
                except Exception as inner_e:
                    self.logger.error(f"Inner error during numeric decryption: {inner_e}")
                    raise
            else:
                self.logger.error(f"Invalid encrypted numeric value type: {type(encrypted_value)}")
                return None
        except Exception as e:
            self.logger.error(f"Error in numeric decryption: {e}")
            try:
                return self._simplified_decrypt(encrypted_value, "numeric")
            except Exception as fallback_e:
                self.logger.error(f"Fallback decryption also failed: {fallback_e}")
                return None

    def encrypt_string(self, value):

        try:
            if not self.context:
                raise ValueError("Encryption context not properly initialized")

            if not isinstance(value, str):
                value = str(value)

            ascii_values = [ord(c) for c in value]

            encrypted_vector = ts.ckks_vector(self.context, ascii_values)

            serialized = encrypted_vector.serialize()

            return serialized
        except Exception as e:
            self.logger.error(f"Error in string encryption: {e}")
            return self._simplified_encrypt(value, "string")

    def decrypt_string(self, encrypted_value):

        try:
            if not self.secret_context:
                raise ValueError("Secret context not available for decryption")

            if not self.secret_context.is_private():
                raise ValueError("Secret context does not have a private key")

            if isinstance(encrypted_value, bytes):
                encrypted_vector = ts.ckks_vector_from(self.secret_context, encrypted_value)

                decrypted_values = encrypted_vector.decrypt()

                chars = [chr(round(val)) for val in decrypted_values]
                return ''.join(chars)
            else:
                self.logger.error(f"Invalid encrypted string value type: {type(encrypted_value)}")
                return None
        except Exception as e:
            self.logger.error(f"Error in string decryption: {e}")
            try:
                return self._simplified_decrypt(encrypted_value, "string")
            except:
                return None

    def _simplified_encrypt(self, value, value_type="string"):

        try:
            if value_type == "numeric":
                prefix = b"NUM:"
                data = str(value).encode('utf-8')
            else:
                prefix = b"STR:"
                data = str(value).encode('utf-8')

            key_bytes = self.symmetric_key if hasattr(self, 'symmetric_key') else b'testkey123'
            encrypted = bytearray()
            for i in range(len(data)):
                encrypted.append(data[i] ^ key_bytes[i % len(key_bytes)])

            result = prefix + bytes(encrypted)
            return result
        except Exception as e:
            self.logger.error(f"Error in simplified encryption: {e}")
            return b"ENCRYPTION_ERROR"

    def _simplified_decrypt(self, encrypted_value, value_type="string"):

        try:
            if not isinstance(encrypted_value, bytes):
                return str(encrypted_value)

            if encrypted_value.startswith(b"NUM:"):
                encrypted_data = encrypted_value[4:]
                is_numeric = True
            elif encrypted_value.startswith(b"STR:"):
                encrypted_data = encrypted_value[4:]
                is_numeric = False
            else:
                encrypted_data = encrypted_value
                is_numeric = (value_type == "numeric")

            key_bytes = self.symmetric_key if hasattr(self, 'symmetric_key') else b'testkey123'
            decrypted = bytearray()
            for i in range(len(encrypted_data)):
                decrypted.append(encrypted_data[i] ^ key_bytes[i % len(key_bytes)])

            if is_numeric:
                value_str = decrypted.decode('utf-8')
                return float(value_str) if '.' in value_str else int(value_str)
            else:
                return decrypted.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error in simplified decryption: {e}")
            return None

    def perform_encrypted_addition(self, encrypted_value1, encrypted_value2):
        try:
            if not self.use_encryption:
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)
                return self.encrypt_value(val1 + val2, "accounts.balance")

            vec1 = ts.ckks_vector_from(self.context, encrypted_value1)
            vec2 = ts.ckks_vector_from(self.context, encrypted_value2)

            result = vec1 + vec2

            return result.serialize()
        except Exception as e:
            self.logger.error(f"Error in encrypted addition: {e}")
            try:
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)
                return self.encrypt_value(val1 + val2, "accounts.balance")
            except:
                return None

    def perform_encrypted_multiplication(self, encrypted_value, scalar):
        try:
            if not self.use_encryption:
                val = self.decrypt_value(encrypted_value)
                return self.encrypt_value(val * scalar, "accounts.balance")

            vec = ts.ckks_vector_from(self.context, encrypted_value)

            result = vec * scalar

            return result.serialize()
        except Exception as e:
            self.logger.error(f"Error in encrypted multiplication: {e}")
            try:
                val = self.decrypt_value(encrypted_value)
                return self.encrypt_value(val * scalar, "accounts.balance")
            except:
                return None

    def compare_encrypted_values(self, encrypted_value1, encrypted_value2, operation="=="):

        try:
            if not self.use_encryption or not self.secret_context:
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)

                if operation == "==":
                    if isinstance(val1, (float, int)) and isinstance(val2, (float, int)):
                        return abs(val1 - val2) < 1e-6
                    else:
                        return val1 == val2
                elif operation == ">":
                    return val1 > val2
                elif operation == "<":
                    return val1 < val2
                else:
                    self.logger.error(f"Unsupported comparison operation: {operation}")
                    return None

            try:
                vec1 = ts.ckks_vector_from(self.secret_context, encrypted_value1)
                vec2 = ts.ckks_vector_from(self.secret_context, encrypted_value2)
            except Exception as e:
                self.logger.error(f"Error deserializing encrypted values: {e}")
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)

                if operation == "==":
                    return abs(val1 - val2) < 1e-6 if isinstance(val1, (float, int)) else val1 == val2
                elif operation == ">":
                    return val1 > val2
                elif operation == "<":
                    return val1 < val2
                else:
                    return None

            if operation == "==":
                diff_vec = vec1 - vec2
                diff_decrypted = diff_vec.decrypt()

                if diff_decrypted and len(diff_decrypted) > 0:
                    diff = abs(diff_decrypted[0])
                    return diff < 1e-4
                return False

            elif operation == ">":
                diff_vec = vec1 - vec2
                diff_decrypted = diff_vec.decrypt()

                if diff_decrypted and len(diff_decrypted) > 0:
                    return diff_decrypted[0] > 1e-6
                return False

            elif operation == "<":
                diff_vec = vec1 - vec2
                diff_decrypted = diff_vec.decrypt()

                if diff_decrypted and len(diff_decrypted) > 0:
                    return diff_decrypted[0] < -1e-6
                return False

            else:
                self.logger.error(f"Unsupported comparison operation: {operation}")
                return None

        except Exception as e:
            self.logger.error(f"Error in encrypted comparison: {e}")
            try:
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)

                if operation == "==":
                    return abs(val1 - val2) < 1e-6 if isinstance(val1, (float, int)) else val1 == val2
                elif operation == ">":
                    return val1 > val2
                elif operation == "<":
                    return val1 < val2
                else:
                    return None
            except Exception as fallback_e:
                self.logger.error(f"Fallback comparison also failed: {fallback_e}")
                return None

    def aggregate_encrypted_values(self, encrypted_values, operation="sum"):
        if not encrypted_values:
            return None

        try:
            if not self.use_encryption:
                values = [self.decrypt_value(v) for v in encrypted_values]
                if operation == "sum":
                    result = sum(values)
                elif operation == "avg":
                    result = sum(values) / len(values)
                else:
                    return None
                return self.encrypt_value(result, "accounts.balance")

            result = ts.ckks_vector_from(self.context, encrypted_values[0])

            for i in range(1, len(encrypted_values)):
                vec = ts.ckks_vector_from(self.context, encrypted_values[i])
                result += vec

            if operation == "avg" and len(encrypted_values) > 1:
                result *= (1.0 / len(encrypted_values))

            return result.serialize()
        except Exception as e:
            self.logger.error(f"Error in encrypted aggregation: {e}")
            try:
                values = [self.decrypt_value(v) for v in encrypted_values]
                if operation == "sum":
                    result = sum(values)
                elif operation == "avg":
                    result = sum(values) / len(values)
                else:
                    return None
                return self.encrypt_value(result, "accounts.balance")
            except:
                return None