import logging
import os
import json
import numpy as np
import base64
from datetime import datetime
import tenseal as ts



class HomomorphicEncryptionManager:
    def __init__(self, key_size=2048, context_params=None, keys_dir="encryption_keys"):
        self.logger = logging.getLogger(__name__)
        self.key_size = key_size
        self.keys_dir = keys_dir

        os.makedirs(self.keys_dir, exist_ok=True)

        _default_ctx = {
            "poly_modulus_degree": 8192,
            "coeff_mod_bit_sizes": [60, 40, 40, 60],
            "scale_bits": 40,
            # you can also include a default plain_modulus here if you like
        }

        # start from the defaults, then overlay any user-provided overrides
        if context_params:
            _default_ctx.update(context_params)

        self.context_params = _default_ctx

        self.sensitive_fields = {
            "traders.email": "string",
            "traders.phone": "string",
            "brokers.contact_email": "string",
            "brokers.license_number": "string",
            "accounts.balance": "numeric"
        }

        self.use_encryption = True

        self.ckks_context = None
        self.secret_context = None
        self.public_key = None
        self.private_key = None

        if self.use_encryption:
            self._initialize_encryption()

        self.bfv_context = None

        try:
            self.bfv_context = ts.context(
                ts.SCHEME_TYPE.BFV,
                poly_modulus_degree=self.context_params["poly_modulus_degree"],
                plain_modulus=self.context_params.get("plain_modulus", 1032193),
                coeff_mod_bit_sizes=self.context_params["coeff_mod_bit_sizes"],
            )
            self.bfv_context.generate_galois_keys()
            self.bfv_context.generate_relin_keys()
            self.logger.info("BFV context initialized for string encryption")
        except Exception as e:
            self.logger.error(f"Failed to initialize BFV context: {e}")


    def _initialize_encryption(self):
        try:
            if self.load_encryption_data():
                self.logger.info("Successfully loaded existing encryption data")
            else:
                self.logger.info("Creating new encryption context and keys")
                self._setup_new_encryption()
        except Exception as e:
            self.logger.error(f"Error initializing encryption: {e}")
            self._setup_simplified_encryption()

        # ── DEBUG SELF-TEST ──
        try:
            plain = 123.456
            self.logger.info(f"Encryption self-test: encrypting {plain}")
            # create a CKKS vector of length 1
            vec = ts.ckks_vector(self.ckks_context, [plain])
            blob = vec.serialize()
            # deserialize under the secret context and decrypt
            loaded = ts.ckks_vector_from(self.secret_context, blob)
            result = loaded.decrypt()[0]
            self.logger.info(f"Encryption self-test: decrypted back {result}")
        except Exception as e:
            self.logger.error(f"Encryption self-test failed: {e}")

    def load_encryption_data(self) -> bool:
        key_path = os.path.join(self.keys_dir, "private_key.dat")
        if not os.path.exists(key_path):
            self.logger.info("No private key context found; will create new one")
            return False

        try:
            self.logger.info("Loading full (secret) CKKS context from private_key.dat")
            with open(key_path, 'rb') as f:
                private_bytes = f.read()

            # rehydrate a real tenseal.Context with the secret key
            full_ctx = ts.context_from(private_bytes)
            self.secret_context = full_ctx

            # derive public-only for encryption
            self.ckks_context = full_ctx.copy()
            self.ckks_context.make_context_public()

            self.logger.info("Successfully loaded secret CKKS context and derived public encryption context")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load private CKKS context: {e}")
            return False

    def _setup_new_encryption(self):
        try:
            poly_modulus_degree = self.context_params.get("poly_modulus_degree", 8192)
            coeff_mod_bit_sizes = self.context_params.get("coeff_mod_bit_sizes", [60, 40, 40, 60])
            scale_bits = self.context_params.get("scale_bits", 40)

            self.logger.info("Creating new TenSEAL CKKS context")
            self.ckks_context = ts.context(
                ts.SCHEME_TYPE.CKKS,
                poly_modulus_degree=poly_modulus_degree,
                coeff_mod_bit_sizes=coeff_mod_bit_sizes
            )

            self.ckks_context.global_scale = 2 ** scale_bits

            self.ckks_context.generate_galois_keys()

            self.secret_context = self.ckks_context.copy()
            # SAVE SECRET BEFORE PUBLICIZING
            self._save_encryption_data()

            # now turn the original ckks_context public
            self.ckks_context.make_context_public()

            self.logger.info("Successfully created and saved new encryption context and keys")
            return True
        except Exception as e:
            self.logger.error(f"Error setting up new encryption: {e}")
            return False

    def _save_encryption_data(self):
        # write the secret‐bearing context first
        secret_path = os.path.join(self.keys_dir, "private_key.dat")
        self.logger.info(f"Saving secret CKKS context to {secret_path}")
        with open(secret_path, "wb") as f:
            # include the secret key in the serialization
            f.write(self.secret_context.serialize(save_secret_key=True))

        # optionally, if you want a public copy on disk too:
        public_path = os.path.join(self.keys_dir, "public_context.dat")
        self.logger.info(f"Saving public CKKS context to {public_path}")
        with open(public_path, "wb") as f:
            f.write(self.ckks_context.serialize())

        self.logger.info("Encryption data saved successfully")

    def _setup_simplified_encryption(self):
        self.logger.warning("Setting up simplified encryption as fallback")
        self.use_encryption = False
        self.ckks_context = None
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
            if not self.use_encryption or not self.ckks_context:
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

        # in encryption_manager.py

    def decrypt_value(self, encrypted_value, field_name=None):
        if encrypted_value is None:
            return None

        field_type = self._get_field_type(field_name)

        try:
            if not self.use_encryption or not self.secret_context:
                # no more XOR-fallback on missing context
                return None

            if field_type == "numeric":
                return self.decrypt_numeric(encrypted_value)
            else:
                return self.decrypt_string(encrypted_value)

        except Exception as e:
            self.logger.error(f"Error decrypting value for field {field_name}: {e}")
            return None

    def _get_field_type(self, field_name):
            if not field_name:
                return "string"

            return self.sensitive_fields.get(field_name, "string")

    def encrypt_numeric(self, value):
        try:
            self.logger.info(f"HE: encrypt_numeric start – value={value}")
            if not self.secret_context or not self.ckks_context:
                raise ValueError("Encryption context not properly initialized")
            if not self.secret_context.is_private():
                raise ValueError("Secret context does not have a private key")

            value = float(value)
            encrypted_vector = ts.ckks_vector(self.ckks_context, [value])
            serialized = encrypted_vector.serialize()

            self.logger.debug(f"HE: encrypt_numeric done – ciphertext bytes={len(serialized)}")
            return serialized
        except Exception as e:
            self.logger.error(f"Error in numeric encryption: {e}")
            return self._simplified_encrypt(value, "numeric")

    def decrypt_numeric(self, encrypted_value):
        """
        Decrypt a CKKS‐encrypted numeric (single‐slot vector) and un‐scale it.
        """
        if encrypted_value is None:
            return None

        if not self.secret_context or not self.secret_context.is_private():
            self.logger.error("Secret context missing or not private")
            return None

        try:
            self.logger.info("HE: decrypt_numeric start")
            self.logger.info(f"Encrypted value length: {len(encrypted_value)} bytes")
            self.logger.info(f"Encrypted value preview: {encrypted_value[:16]}…")

            # Deserialize and decrypt
            vec = ts.ckks_vector_from(self.secret_context, encrypted_value)
            raw = vec.decrypt()[0]
            rounded=round(raw, 2)
            self.logger.info(f"Decrypted plaintext value: {rounded} (rounded)")
            return rounded

        except Exception as e:
            self.logger.error(f"HE-BFV: numeric decrypt failed: {e}")
            return None

    def encrypt_string(self, value: str) -> bytes:
        if value is None:
            return None
        codepoints = [ord(ch) for ch in value]
        enc = ts.bfv_vector(self.bfv_context, codepoints)
        token = enc.serialize()
        self.logger.info(f"HE-BFV: encrypted string of length {len(value)} -> {len(token)} bytes")
        return token

    def decrypt_string(self, token: bytes) -> str:
        if token is None:
            return None

        try:
            vec = ts.bfv_vector_from(self.bfv_context, token)
            decrypted_ints = vec.decrypt()

            chars = []
            for v in decrypted_ints:
                code = int(v)

                # drop padding
                if code == 0:
                    continue

                # only valid Unicode scalars
                if not (0 <= code <= 0x10FFFF):
                    continue

                # (optional) restrict to printable ASCII for emails
                if not (32 <= code <= 126):
                    continue

                chars.append(chr(code))

            return "".join(chars)
        except Exception as e:
            self.logger.error(f"HE-BFV: string decrypt failed: {e}")
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

            # only decode the tiny XOR‐based scheme that we ourselves prefix
            if encrypted_value.startswith(b"NUM:"):
                encrypted_data = encrypted_value[4:]
                is_numeric = True
            elif encrypted_value.startswith(b"STR:"):
                encrypted_data = encrypted_value[4:]
                is_numeric = False
            else:
                # this is almost certainly a CKKS blob—don't try to UTF-8 it!
                self.logger.error(
                    "Simplified decrypt: unsupported ciphertext format, skipping fallback"
                )
                return None

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
            self.logger.info("HE: starting homomorphic addition")
            if not self.use_encryption:
                val1 = self.decrypt_value(encrypted_value1)
                val2 = self.decrypt_value(encrypted_value2)
                return self.encrypt_value(val1 + val2, "accounts.balance")

            vec1 = ts.ckks_vector_from(self.ckks_context, encrypted_value1)
            vec2 = ts.ckks_vector_from(self.ckks_context, encrypted_value2)

            result = vec1 + vec2
            self.logger.debug(f"HE: addition result ciphertext={len(result.serialize())}")
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
            self.logger.info(f"HE: starting homomorphic multiplication by scalar={scalar}")
            if not self.use_encryption:
                val = self.decrypt_value(encrypted_value)
                return self.encrypt_value(val * scalar, "accounts.balance")

            vec = ts.ckks_vector_from(self.ckks_context, encrypted_value)

            result = vec * scalar
            self.logger.debug(f"HE: multiplication result ciphertext={len(result.serialize())}")
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

            result = ts.ckks_vector_from(self.ckks_context, encrypted_values[0])

            for i in range(1, len(encrypted_values)):
                vec = ts.ckks_vector_from(self.ckks_context, encrypted_values[i])
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