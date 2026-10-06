import os
import unittest
from unittest.mock import patch
from kafka_security import ssl_options

class TransportConfigTests(unittest.TestCase):
    def test_required_mtls_rejects_plaintext_and_partial_config(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                ssl_options(require=True)
        with patch.dict(os.environ, {'KAFKA_SSL_CERT': 'cert.pem'}, clear=True):
            with self.assertRaises(ValueError):
                ssl_options()

    def test_full_config_keeps_hostname_verification(self):
        with patch.dict(os.environ, {'KAFKA_SSL_CA': 'ca.pem', 'KAFKA_SSL_CERT': 'cert.pem',
            'KAFKA_SSL_KEY': 'key.pem'}, clear=True):
            config = ssl_options(require=True)
            self.assertEqual(config['security_protocol'], 'SSL')
            self.assertIs(config['ssl_check_hostname'], True)

if __name__ == '__main__':
    unittest.main()
